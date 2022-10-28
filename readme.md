# finance kit

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

### 华泰期权计算

#### 获取华泰期权交易日价格与波动率参数

期权需要配置参数

1. `--date` 最新交易日
2. `--overwrite` 是否覆盖当前已有数据
3. `-d`, `--directory`，爬取的数据输出存储目录
4. `-s`, `--source`，采集目标的数据源名称

```shell
finkit collector -s huatai-option -d ./ --overwrite
```

#### 计算华泰期权波动率与价格

1. `--date` 收盘价价格日期，若该日期不是交易日，则可能获取之前最近一个交易日期数据，默认当前日期
2. `-d`, `--directory`，中间数据及结果输出存储目录，默认`./data`
3. `-s`, `--source`，待计算的数据名称，目前支持`huatai-option`
4. `--contract`，合约名称，例如 `au2212.shfe`，`au2212`
5. `--strike-price`，期权执行价，可以输入多个，以逗号隔开，例如 `396,399`
6. `--strike-date`，期权行权日，可以输入多个，例如 `2022-11-23,2022-11-24`
7. `--rate`，无风险利率，默认 `0.03`
8. `--dividend-rate`，股息利率，默认`0`
9. `--method`，期权计算方法，当前仅支持欧式期权`bsm`方法

```shell
finkit option --source huatai-option \
  --contract au2212.shfe \
  --strike-price 396,399 \
  --strike-date 2022-11-23,2022-11-24 \
  --rate 0.03 --dividend-rate 0.03
```

`finkit option`指调用option计算方法，该方法会自动下载并存储中间数据，若中间数据存在时，则直接计算期权价格

### 上海期货日交易量

```shell
finkit collector -s daily-volume-shfe --date 2022-10-28
```

