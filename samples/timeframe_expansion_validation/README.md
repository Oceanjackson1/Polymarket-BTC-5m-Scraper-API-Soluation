# Timeframe Expansion Validation Samples

用于验证“从 5m 扩展到 5m/15m/1h/4h”后的核心逻辑正确性。

## 文件
- `input_markets.json`：市场输入样本（含 4 条合法 + 2 条非法）
- `expected_markets_all_timeframes.csv`：四周期启用时应保留的标准化市场输出
- `sample_trades_multi_timeframes.csv`：四周期成交样本（含毫秒字段）
- `expected_output_paths.json`：不同 `--timeframes` 下输出文件命名预期
- `validate_logic.py`：自动校验脚本

## 执行校验
```bash
python3 samples/timeframe_expansion_validation/validate_logic.py
```

预期输出：`timeframe_expansion_validation_passed`
