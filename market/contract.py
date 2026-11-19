"""共享契约：申请、身份哈希、筛选与投影的确定解释。

契约 = 投影列、逻辑类型（含 NULL 处理）与筛选条件。筛选/列/类型任何变化
都会得到不同的契约身份，旧传递不得用于新条件，必须重新申请并被接收方明确接纳。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .hashing import canonical_json, sha256_hex

# pyarrow 逻辑类型映射（仅声明契约允许的类型）
import pyarrow as pa

_PA_TYPES = {
    "string": pa.string(),
    "decimal(18,3)": pa.decimal128(18, 3),
}

# DuckDB 读取源文件后的 cast 表达式（源 weight 为字符串，定点转 decimal，不走浮点）
_DUCK_CAST = {
    "string": "CAST({col} AS VARCHAR)",
    "decimal(18,3)": "CAST({col} AS DECIMAL(18,3))",
}


@dataclass(frozen=True)
class ShareContract:
    id: str
    columns: tuple[str, ...]
    types: tuple[str, ...]
    where: Mapping[str, Any]

    @classmethod
    def from_file(cls, path) -> "ShareContract":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ShareContract":
        contract = cls(
            id=str(raw["id"]),
            columns=tuple(raw["columns"]),
            types=tuple(raw["types"]),
            where=dict(raw.get("where", {})),
        )
        if len(contract.columns) != len(contract.types):
            raise ValueError("契约列与类型数量不一致")
        unknown = [t for t in contract.types if t not in _PA_TYPES]
        if unknown:
            raise ValueError(f"契约含未支持类型: {unknown}")
        if not contract.columns:
            raise ValueError("共享契约至少需要一列")
        if len(set(contract.columns)) != len(contract.columns):
            raise ValueError("契约投影列存在重复")
        return contract

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "columns": list(self.columns),
                "types": list(self.types), "where": dict(self.where)}

    def identity_hash(self) -> str:
        """契约身份：列、类型、筛选任一变化都会改变该哈希。"""
        return sha256_hex(canonical_json(self.to_dict()))

    @property
    def arrow_schema(self) -> pa.Schema:
        return pa.schema([pa.field(c, _PA_TYPES[t]) for c, t in zip(self.columns, self.types)])

    def _where_sql(self) -> str:
        clauses = []
        if "district_in" in self.where:
            values = self.where["district_in"]
            if not isinstance(values, list) or not values:
                raise ValueError("district_in 必须是非空列表")
            literals = ", ".join("?" for _ in values)
            clauses.append(f"district IN ({literals})")
        if not clauses:
            return "TRUE", []
        return " AND ".join(clauses), list(self.where["district_in"])

    def view_select_sql(self, source: str) -> tuple[str, list[Any]]:
        """对一个 parquet 源（文件名参数或表名）生成投影+筛选 SQL。"""
        where_sql, params = self._where_sql()
        select = ", ".join(
            _DUCK_CAST[t].format(col=c) for c, t in zip(self.columns, self.types)
        )
        return f"SELECT {select} FROM {source} WHERE {where_sql}", params

    def project_files(self, con, files: Sequence[Path]) -> list[dict[str, Any]]:
        """按契约读取一批原件，返回投影筛选后的 Python 行（Decimal 精确值）。"""
        rows: list[dict[str, Any]] = []
        for path in files:
            sql, params = self.view_select_sql("read_parquet(?)")
            fetched = con.execute(sql, [str(path), *params]).fetchall()
            for tup in fetched:
                rows.append(dict(zip(self.columns, tup)))
        return rows

    def rows_to_arrow(self, rows: Sequence[Mapping[str, Any]]) -> pa.Table:
        """按契约精确 schema 构造 Arrow 表（weight 为 DECIMAL128，非浮点）。"""
        columns: dict[str, list[Any]] = {c: [] for c in self.columns}
        for row in rows:
            for c, t in zip(self.columns, self.types):
                value = row.get(c)
                if value is not None and t.startswith("decimal"):
                    from decimal import Decimal
                    value = Decimal(str(value))
                columns[c].append(value)
        return pa.table(columns, schema=self.arrow_schema)
