# 无业务主键集市的差量共享接收

本项目提供合成回收业务原件及物理检查入口，并在其上实现提供方共享、净变化生成、
本地文件交接、接收方原子入仓与双方对账。两个仓各自落真实 Parquet 与 DuckDB
元数据库，独立进程可查；产品实现见 `market/`，操作手册见 [docs/operations.md](docs/operations.md)。

使用 Python 3.11，依赖版本固定在 requirements.txt。安装命令为 python -m pip install -r requirements.txt。运行 python -m materials inputs 校验清单、摘要、Parquet行数并实际使用DuckDB读取；运行 python -m unittest discover -s tests -v 检查独立JSON值、原件损坏及业务材料形状。

产品命令统一从仓库根入口运行（隔离模式，自动处理 sys.path）：

- `python3 -I run_tests.py`：全部 25 个测试（协议基元 + 两仓端到端，覆盖断线重送、重复份数、物理重排、完整替换衔接、冲突与契约变更）；
- `python3 -I scenario.py`：按 `inputs/actions.json` 五步驱动的完整演示；
- `python3 -I run.py --help`：共享申请、snapshot/delta/rebase、send/receive、查询与对账等子命令。

可执行 docker build -t recycling-inputs .，再执行 docker run --rm recycling-inputs；测试命令为 docker run --rm recycling-inputs python -m unittest discover -s tests -v。环境不依赖外部业务站点。资料均为本地合成，不含真实个人、企业数据或凭据。

inputs/seed.json与同名Parquet是相同输入的两种载体；inputs/actions.json提供尚未应用的后续资料。docs/input-contract.md解释字段及原有业务约定。materials.physical只校验物理文件，不裁决身份、金额、匹配或有效版本。materials.build_inputs仅重建输入Parquet和物理清单，通常无需运行；不要把其产物误当未来业务结果。

产品输出默认写入 work/（运行产物，已在 .gitignore），原始资料保留供回查；market 包的入仓语义、轮次留档、冲突与代际守卫以 docs/operations.md 为准。
