"""本地文件交接与共享申请。

不依赖真实网络：
- handoff/applications/<id>.json  接收方提交、提供方接纳的共享申请；
- handoff/packages/<delivery_id>/ 提供方放出的自校验传递包；
- handoff/acks/<delivery_id>.json 接收方持久化确认。

断点全部由显式参数注入，便于验收“在持久化与确认之间断线”。
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path

from .hashing import canonical_json, sha256_hex
from .store import atomic_write_text


class Handoff:
    def __init__(self, root):
        self.root = Path(root).resolve()
        (self.root / "applications").mkdir(parents=True, exist_ok=True)
        (self.root / "packages").mkdir(parents=True, exist_ok=True)
        (self.root / "acks").mkdir(parents=True, exist_ok=True)

    def submit_application(self, contract_path, applicant: str = "county-warehouse") -> Path:
        """接收方提交共享申请（申请含完整契约，不靠文件名推断条件）。"""
        contract = json.loads(Path(contract_path).read_text(encoding="utf-8"))
        application = {
            "application_id": "app-" + uuid.uuid4().hex[:12],
            "applicant": applicant,
            "contract": contract,
        }
        application["application_hash"] = sha256_hex(
            canonical_json({k: v for k, v in application.items()
                            if k != "application_hash"})
        )
        path = self.root / "applications" / f"{application['application_id']}.json"
        atomic_write_text(path, json.dumps(application, ensure_ascii=False,
                                           indent=2, sort_keys=True))
        return path

    def list_applications(self) -> list[dict]:
        out = []
        for p in sorted((self.root / "applications").glob("*.json")):
            out.append(json.loads(p.read_text(encoding="utf-8")))
        return out

    def package_dir(self, delivery_id: str) -> Path:
        return self.root / "packages" / delivery_id

    def ack_path(self, delivery_id: str) -> Path:
        return self.root / "acks" / f"{delivery_id}.json"
