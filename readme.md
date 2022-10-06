# finance kit

```shell
python main.py collector -s huatai_option --date 2022-09-30 -d ./ --overwrite
```

## build and package

使用最新的 `pyproject.toml` 方式管理项目，依赖 `setuptools`, `python3.9+`

```shell
pip install build

python -m build
```

## install

```shell
# 确定系统python3.9+
virtualenv venv

pip install finkit-0.0.1-py3-none-any.whl
```

## execution

### 获取华泰期权交易日价格与波动率

期权需要配置参数

1. `--date` 最新交易日
2. `--overwrite` 是否覆盖当前已有数据
