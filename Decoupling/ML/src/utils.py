from typing import Any, Literal, Callable, Tuple, Awaitable, Optional, cast, Iterable

from config import *

import sys
import uuid
from time import time, sleep
from datetime import datetime, timedelta
from queue import Queue
import asyncio
from concurrent import futures
from itertools import combinations

import pandas as pd
import numpy as np
from statsmodels.tsa.api import adfuller, VAR, coint
from statsmodels.tsa.vector_ar import vecm
from statsmodels.tsa.arima.model import ARIMA
from outliers import smirnov_grubbs as grubbs
from pyod.models import lof, cof, inne, ecod, lscp
import sranodec as anom

import redis.asyncio as redis
import aiomqtt

import orjson as json
from dependency_injector import containers, providers
from dependency_injector.wiring import Provide, inject

if sys.platform.lower() == "win32" or os.name.lower() == "nt":
    from asyncio import set_event_loop_policy, WindowsSelectorEventLoopPolicy

    set_event_loop_policy(WindowsSelectorEventLoopPolicy())

import warnings

warnings.filterwarnings("ignore")

INTERVAL = 10
HISTORY_STEP = 25
FORECAST_STEP = 5
FORECAST_INTERVAL = 60
DECIMALS = 3
ADFULLER_THRESHOLD_VAL = 0.05
ENGLE_GRANGER_THRESHOLD_VAL = 0.05
ADFULLER_CONFIDENCE_INTERVAL = 5
MAX_QUEUE_SIZE = 10

ANOMALY_SCORE = 50
ALERT_SCORE = 50
PER_ALERT_SCORE = 2.5
ANOMALY_COL_RATIO = 3
GRUBBS_SCORE = 10
GRUBBS_ANOM_COL_RATIO = 0.12
LSCP_SCORE = 30
LSCP_ANOM_COL_RATIO = 0.12
SRA_SCORE = 10
SRA_ANOM_COL_RATIO = 0.01

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
PROJECT_NAME = "HOW"

H2_PRESSURE_LOW = 0.44
H2_PURITY_LOW = 95
H2_PURITY_LOWLOW = 92
H2_LEAKAGE_HIGH = 4
OIL_H2_PRESSURE_DIFF_LOW = 36
OIL_H2_PRESSURE_DIFF_HIGH = 76
WATER_FLOW_LOW = 22
WATER_CONDUCTIVITY_HIGH = 0.5
WATER_CONDUCTIVITY_HIGHHIGH = 2

H2Leakage = (
    "发电机内冷水箱氢气泄漏",
    "发电机密封油励侧回油氢气泄漏",
    "发电机密封油汽侧回油氢气泄漏",
    "发电机封闭母线A相氢气泄漏",
    "发电机封闭母线B相氢气泄漏",
    "发电机封闭母线C相氢气泄漏",
    "发电机封闭母线中性点1氢气泄漏",
    "发电机封闭母线中性点2氢气泄漏",
)

otherFTargets = (
    "进氢压力",
    "发电机内氢气纯度",
    # "发电机出氢湿度",
    "发电机密封油-氢气差压",
    # "发电机密封油油含水量",
    "发电机定子冷却水电导率",
    # "发电机定子冷却水油水PH计",
    "发电机定子冷却水流量1差压",
)

H2Targets = [
    "time",
    "发电机内冷水箱氢气泄漏",
    "发电机密封油励侧回油氢气泄漏",
    "发电机密封油汽侧回油氢气泄漏",
    "发电机封闭母线A相氢气泄漏",
    "发电机封闭母线B相氢气泄漏",
    "发电机封闭母线C相氢气泄漏",
    "发电机封闭母线中性点1氢气泄漏",
    "发电机封闭母线中性点2氢气泄漏",
    "进氢压力",
    "发电机内氢气纯度",
    "发电机出氢湿度",
    "发电机氢干燥装置后氢气湿度",
]

oilTargets = [
    "time",
    "10#轴承回油温度",
    "4#轴承回油温度",
    "9#轴承回油温度",
    "11#轴承回油温度",
    "发电机密封油-氢气差压",
    "机组发电机密封油汽端压力",
    "机组发电机密封油励端压力",
    "发电机密封油油含水量",
    "B浮子油箱油位",
    "发电机密封油主密封油泵A电流",
    "发电机密封油主密封油泵B电流",
    "发电机密封油事故密封油泵电流",
]

waterTargets = [
    "time",
    "发电机定子线棒层间温度1",
    "发电机定子铁芯温度1",
    "发电机定子铜屏蔽温度1",
    "发电机定子线圈出水温度1",
    "发电机定子冷却水出水温度1",
    "发电机定子冷却水入水温度1",
    "发电机定子冷却水温度调节阀控制指令",
    "发电机定子冷却水电导率",
    "发电机离子交换器出水电导率",
    "发电机定子冷却水油水PH计",
    "发电机定子冷却水流量1差压",
    "机组发电机引出线流量",
]

n = 2
f_df = [pd.DataFrame() for _ in range(n)]
a_df = [pd.DataFrame() for _ in range(n)]
status = [{} for _ in range(n)]
alerts = [{"alarms": [], "timestamp": ""} for _ in range(n)]
vrfData = [{} for _ in range(n)]
arfData = [{} for _ in range(n)]
healthQ = [Queue(maxsize=MAX_QUEUE_SIZE) for _ in range(n)]
currentTime = [datetime.now() for _ in range(n)]
now = ["" for _ in range(n)]


class RedisService:

    def __init__(self, redis: redis.Redis) -> None:
        self._redis = redis

    async def hget(self, name: str, key: str) -> Awaitable[Optional[str]]:
        return await self._redis.hget(name, key)  # type: ignore

    async def hgetall(self, name: str) -> Awaitable[dict]:
        return await self._redis.hgetall(name)  # type: ignore

    async def hset(self, name: str, key: str, value: str) -> Awaitable[int]:
        return await self._redis.hset(name, key, value)  # type: ignore


class RedisContainer(containers.DeclarativeContainer):
    # config = providers.Configuration()

    def _init_redis() -> redis.Redis:  # type: ignore
        pool = redis.ConnectionPool.from_url(
            f"redis://{REDIS_USERNAME}:{REDIS_PASSWORD}@{REDIS_IP}:{REDIS_PORT}/{REDIS_DB}",
            encoding="utf-8",
            decode_responses=True,
        )
        conn = redis.Redis.from_pool(pool)
        print("----build new Redis connection----")
        return conn

    _redis_conn = providers.Resource(
        _init_redis,
    )

    service = providers.Factory(
        RedisService,
        redis=_redis_conn,
    )


class AppContainer(containers.DeclarativeContainer):
    redis_package = providers.Container(RedisContainer)


class GetData:

    def __init__(self, unit: str) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1
        self.fTargets = list(H2Leakage + otherFTargets)
        self.all_sample = "static/testData/all.sample"
        self.health_sample = "static/testData/health.sample"

    def get_forecast_df(self) -> None:
        f_df[self.iUnit] = pd.DataFrame()
        f_df[self.iUnit] = pd.read_pickle(self.all_sample)[self.fTargets][:100]

    def get_anomaly_df(self) -> None:
        a_df[self.iUnit] = pd.DataFrame()
        a_df[self.iUnit] = pd.read_pickle(self.health_sample).drop(columns=["time"])[:100]


class AnomalyDetection:

    def __init__(self, unit: str) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1
        self.clf = lscp.LSCP(
            contamination=0.01,
            detector_list=self._create_detector_list(),
            n_bins=len(self._create_detector_list()),
        )

    def _create_detector_list(self) -> list[Any]:
        return [
            lof.LOF(),
            cof.COF(),
            inne.INNE(),
            ecod.ECOD(),
        ]

    def grubbs_t(self) -> float:
        if a_df[self.iUnit].shape[0] < 24:
            print("Not enough data for grubbs")
            return 0

        x = 0
        try:
            for col in a_df[self.iUnit].columns:
                result = grubbs.two_sided_test_indices(
                    a_df[self.iUnit][col].values, alpha=0.05
                )
                if result:
                    x += len(result) > 0

            final = x / a_df[self.iUnit].shape[1]
            return final
        except Exception as e:
            print("grubbs_t exception: ", e)
            return 0

    def lscp_t(self) -> float:
        if a_df[self.iUnit].shape[0] < 45:
            print("Not enough data for lscp")
            return 0

        try:
            self.clf.fit(a_df[self.iUnit])
            result = self.clf.predict(a_df[self.iUnit])

            final = np.sum(result == 1) / a_df[self.iUnit].shape[1]
            return float(final)
        except Exception as e:
            print("lscp_t exception: ", e)
            return 0

    def spectral_residual_saliency(self) -> float:
        if a_df[self.iUnit].shape[0] < 24:
            print("Not enough data for srs")
            return 0

        score_window_size = min(a_df[self.iUnit].shape[0], 100)
        spec = anom.Silency(
            amp_window_size=24,
            series_window_size=24,
            score_window_size=score_window_size,
        )

        try:
            abnormal = 0
            for col in a_df[self.iUnit].columns:
                score = spec.generate_anomaly_score(a_df[self.iUnit][col].values)
                abnormal += np.sum(score > np.percentile(score, 99)) / len(score) > 0.02

            result = abnormal / a_df[self.iUnit].shape[1]
            return float(result)
        except Exception as e:
            print("spectral_residual_saliency exception: ", e)
            return 0


class Forecast:

    def __init__(self, unit: str) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1
        self.targetList = list(H2Leakage)

    def _VECM_forecast(self, data: pd.DataFrame) -> Any:
        # Johansen 检验方法确定协整关系阶数
        rank = vecm.select_coint_rank(data, det_order=0, k_ar_diff=1, signif=0.1)
        model = vecm.VECM(data, k_ar_diff=1, coint_rank=rank.rank)
        yhat = model.fit().predict(steps=FORECAST_STEP)
        # print(yhat)
        return yhat

    def _VAR_forecast(self, data: pd.DataFrame) -> Any:
        # data的索引必须是递增的 data = data.sort_index()
        model = VAR(data, exog=None)
        # 数据量太小maxlags选择10会报错，数据量不变维度多也会报错
        res = model.select_order(maxlags=5)
        best_lag = res.selected_orders["aic"]
        # print(f"best_lag={best_lag}")
        yhat = model.fit(maxlags=int(max(best_lag, 10))).forecast(
            data.values, steps=FORECAST_STEP
        )
        # print(yhat)
        return yhat

    def _check_stationarity(self, df: pd.DataFrame) -> Literal[0, 1, 2]:
        for _, column_data in df.items():
            # 常数列
            if column_data.nunique() == 1:
                return 2

            result = adfuller(column_data)

            # 非平稳
            if (
                result[1] > ADFULLER_THRESHOLD_VAL
                or result[4][f"{ADFULLER_CONFIDENCE_INTERVAL}%"] <= result[0]
            ):
                return 0
            else:
                continue
        return 1

    def _check_cointegration(self, df: pd.DataFrame, index: int) -> Literal[0, 1]:
        for col1, col2 in combinations(self.targetList[index], 2):
            result = coint(df[col1], df[col2])

            if result[1] > ENGLE_GRANGER_THRESHOLD_VAL or result[2][1] <= result[0]:
                print(f"{col1}, {col2} : Non-cointegration")
                return 0
        return 1

    def vr_forecast(self) -> None:
        vrfData[self.iUnit] = {}
        if f_df[self.iUnit].shape[0] < 40:
            print("Not enough data for forecast")
            return

        vrdf = f_df[self.iUnit][self.targetList]

        stationarity = self._check_stationarity(vrdf)
        # print(stationarity)
        if stationarity == 2:
            print("Constant column values")
            return

        try:
            if stationarity == 1:
                yhat = self._VAR_forecast(vrdf)
            else:
                # 默认是协整的，使用VEC模型
                # print(self._check_cointegration(vrdf, i))
                yhat = self._VECM_forecast(vrdf)
        except Exception as e:
            print("Exception: ", e)
        else:
            yhat = pd.DataFrame(yhat, columns=vrdf.columns)
            result = pd.concat(
                [vrdf.iloc[-HISTORY_STEP:, :], yhat], axis=0, ignore_index=True
            )
            vrfData[self.iUnit].update(
                {
                    col: result[col].round(DECIMALS).values
                    for col in result.columns[: len(self.targetList)]
                }
            )
        # print(vrfData[self.iUnit])

    def ar_forecast1(self) -> None:
        arfData[self.iUnit] = {}
        if f_df[self.iUnit].shape[0] < 40:
            print("Not enough data for ar_forecast1")
            return

        order = (2, 0, 2)
        yhat = []
        ardf = f_df[self.iUnit][list(otherFTargets)]

        for i in range(ardf.shape[1]):
            model = ARIMA(ardf.iloc[:, i], order=order)
            forecast = model.fit().forecast(steps=FORECAST_STEP)
            yhat.append(forecast)
        yhatdf = pd.concat(yhat, axis=1)
        yhatdf.columns = ardf.columns
        result = pd.concat(
            [ardf.iloc[-HISTORY_STEP:, :], yhatdf], axis=0, ignore_index=True
        )
        arfData[self.iUnit].update(
            {col: result[col].round(DECIMALS) for col in result.columns}
        )
        # print(arfData[self.iUnit].keys())


class Logic:

    @inject  # 可以不要
    def __init__(
        self,
        unit: str,
        conn: RedisContainer = Provide[AppContainer.redis_package.service],
    ) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1
        self.H2_pressure = "进氢压力"
        self.H2_purity = "发电机内氢气纯度"
        self.H2_leakage = H2Leakage
        self.diff_pressure = "发电机密封油-氢气差压"
        self.water_flow = "发电机定子冷却水流量1差压"
        self.conductivity = "发电机定子冷却水电导率"
        self.redis_conn = conn
        self.H2_pressure_key = f"{PROJECT_NAME}{unit}:Forecast:H2_pressure"
        self.H2_purity_key = f"{PROJECT_NAME}{unit}:Forecast:H2_purity"
        self.H2_leakage_key = f"{PROJECT_NAME}{unit}:Forecast:H2_leakage"
        self.diff_pressure_key = f"{PROJECT_NAME}{unit}:Forecast:diff_pressure"
        self.water_flow_key = f"{PROJECT_NAME}{unit}:Forecast:water_flow"
        self.conductivity_key = f"{PROJECT_NAME}{unit}:Forecast:conductivity"

    def getNow(self) -> None:
        global currentTime, now
        currentTime[self.iUnit] = datetime.now()
        now[self.iUnit] = currentTime[self.iUnit].strftime(DATE_FORMAT)

    async def _trigger(
        self, name: str, key: str, tag: str, content: str, st: str
    ) -> None:
        d = dict(code=tag, desc=content, advice="", startTime=st)
        if not st:
            await self.redis_conn.hset(name, key, now[self.iUnit])
            d["startTime"] = now[self.iUnit]

        alerts[self.iUnit]["alarms"].append(d)

    async def _revert(self, name: str, key: str, st: str) -> None:
        if st:
            await self.redis_conn.hset(name, key, 0)

    async def _H2_pressure(self) -> Literal[0, 1]:
        flag = 0
        tag = self.H2_pressure
        st = await self.redis_conn.hget(self.H2_pressure_key, tag)
        tag_forecast = arfData[self.iUnit].get(tag, np.array([]))
        # print("H2 pressure: ", tag_forecast[-FORECAST_STEP:])
        if np.any(tag_forecast[-FORECAST_STEP:] < H2_PRESSURE_LOW) if tag_forecast.size else False:
            flag = 1
            await self._trigger(self.H2_pressure_key, tag, tag, "发电机进氢压力低", st)
        else:
            await self._revert(self.H2_pressure_key, tag, st)
        return flag

    async def _water_flow(self) -> Literal[0, 1]:
        flag = 0
        tag = self.water_flow
        st = await self.redis_conn.hget(self.water_flow_key, tag)
        tag_forecast = arfData[self.iUnit].get(tag, np.array([]))
        # print("water flow: ", tag_forecast[-FORECAST_STEP:])
        if np.any(tag_forecast[-FORECAST_STEP:] < WATER_FLOW_LOW) if tag_forecast.size else False:
            flag = 1
            await self._trigger(self.water_flow_key, tag, tag, "发电机流量低", st)
        else:
            await self._revert(self.water_flow_key, tag, st)
        return flag

    async def _diff_pressure(self) -> Literal[0, 1]:
        flag = 0
        tag = self.diff_pressure
        st1 = await self.redis_conn.hget(self.diff_pressure_key, tag + "_1")
        st2 = await self.redis_conn.hget(self.diff_pressure_key, tag + "_2")
        tag_forecast = arfData[self.iUnit].get(tag, np.array([]))
        # print("diff pressure: ", tag_forecast[-FORECAST_STEP:])
        if not st2 and (
            np.any(tag_forecast[-FORECAST_STEP:] > OIL_H2_PRESSURE_DIFF_HIGH) if tag_forecast.size else False
        ):
            flag = 1
            await self._trigger(self.diff_pressure_key, tag + "_1", tag, "油氢差压高", st1)
        else:
            await self._revert(self.diff_pressure_key, tag + "_1", st1)
        if not st1 and (
            np.any(tag_forecast[-FORECAST_STEP:] < OIL_H2_PRESSURE_DIFF_LOW) if tag_forecast.size else False
        ):
            flag = 1
            await self._trigger(self.diff_pressure_key, tag + "_2", tag, "油氢差压低", st2)
        else:
            await self._revert(self.diff_pressure_key, tag + "_2", st2)
        return flag

    async def _H2_purity(self) -> Literal[0, 1]:
        flag = 0
        tag = self.H2_purity
        st1 = await self.redis_conn.hget(self.H2_purity_key, tag + "_1")
        st2 = await self.redis_conn.hget(self.H2_purity_key, tag + "_2")
        tag_forecast = arfData[self.iUnit].get(tag, np.array([]))
        # print("H2 purity: ", tag_forecast[-FORECAST_STEP:])
        if np.any(tag_forecast[-FORECAST_STEP:] < H2_PURITY_LOW) if tag_forecast.size else False:
            flag = 1
            await self._trigger(self.H2_purity_key, tag + "_1", tag, "氢气纯度低", st1)
        else:
            await self._revert(self.H2_purity_key, tag + "_1", st1)
        if np.any(tag_forecast[-FORECAST_STEP:] < H2_PURITY_LOWLOW) if tag_forecast.size else False:
            flag = 1
            await self._trigger(self.H2_purity_key, tag + "_2", tag, "氢气纯度低低", st2)
        else:
            await self._revert(self.H2_purity_key, tag + "_2", st2)
        return flag

    async def _conductivity(self) -> Literal[0, 1]:
        flag = 0
        tag = self.conductivity
        st1 = await self.redis_conn.hget(self.conductivity_key, tag + "_1")
        st2 = await self.redis_conn.hget(self.conductivity_key, tag + "_2")
        tag_forecast = arfData[self.iUnit].get(tag, np.array([]))
        # print("conductivity: ", tag_forecast[-FORECAST_STEP:])
        if np.any(tag_forecast[-FORECAST_STEP:] > WATER_CONDUCTIVITY_HIGH) if tag_forecast.size else False:
            flag = 1
            await self._trigger(self.conductivity_key, tag + "_1", tag, "定冷水电导率高", st1)
        else:
            await self._revert(self.conductivity_key, tag + "_1", st1)
        if np.any(tag_forecast[-FORECAST_STEP:] > WATER_CONDUCTIVITY_HIGHHIGH) if tag_forecast.size else False:
            flag = 1
            await self._trigger(self.conductivity_key, tag + "_2", tag, "定冷水电导率高高", st2)
        else:
            await self._revert(self.conductivity_key, tag + "_2", st2)
        return flag

    async def _H2_leakage(self) -> Literal[0, 1]:
        flag = 0
        for tag in self.H2_leakage:
            st = await self.redis_conn.hget(self.H2_leakage_key, tag)
            tag_forecast = vrfData[self.iUnit].get(tag, np.array([]))
            # print("H2 leakage: ", tag_forecast[-FORECAST_STEP:])
            if np.any(tag_forecast[-FORECAST_STEP:] > H2_LEAKAGE_HIGH) if tag_forecast.size else False:
                flag = 1
                await self._trigger(self.H2_leakage_key, tag, tag, "发电机漏氢", st)
            else:
                await self._revert(self.H2_leakage_key, tag, st)
        return flag

    async def _foo(self, judgeFunc: Callable[[], Awaitable[Literal[0, 1]]]) -> None:
        tag = judgeFunc.__name__
        r = await judgeFunc()
        status[self.iUnit][tag] = "异常" if r else "未见异常"

    async def analysis(self, MQTTClient: aiomqtt.Client) -> None:
        alerts[self.iUnit] = {"alarms": [], "timestamp": now[self.iUnit]}

        await self._foo(self._H2_pressure)
        await self._foo(self._H2_purity)
        await self._foo(self._conductivity)
        await self._foo(self._diff_pressure)
        await self._foo(self._water_flow)
        await self._foo(self._H2_leakage)

        # print(alerts)
        # print(status)
        await MQTTClient.publish(
            f"{PROJECT_NAME}{self.unit}/Forecast/Status", json.dumps(status[self.iUnit])
        )
        await MQTTClient.publish(
            f"{PROJECT_NAME}{self.unit}/Forecast/Alerts", json.dumps(alerts[self.iUnit])
        )

        timestamp = [
            (
                currentTime[self.iUnit] + timedelta(seconds=FORECAST_INTERVAL * i)
            ).strftime(DATE_FORMAT)
            for i in range(-HISTORY_STEP + 1, FORECAST_STEP + 1)
        ]

        for data_dict, topic in [
            (vrfData[self.iUnit], "VR"),
            (arfData[self.iUnit], "AR"),
        ]:
            data = {k: v.tolist() for k, v in data_dict.items()}
            data["timestamp"] = timestamp
            await MQTTClient.publish(
                f"{PROJECT_NAME}{self.unit}/Forecast/{topic}", json.dumps(data)
            )

class Health:

    @inject
    def __init__(
        self,
        unit: str,
        conn: RedisContainer = Provide[AppContainer.redis_package.service],
    ) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1
        self.redis_conn = conn
        self.keys = [
            f"{PROJECT_NAME}{unit}:Mechanism:H2_pressure",
            f"{PROJECT_NAME}{unit}:Mechanism:H2_purity",
            f"{PROJECT_NAME}{unit}:Mechanism:conductivity",
            f"{PROJECT_NAME}{unit}:Mechanism:diff_pressure",
            f"{PROJECT_NAME}{unit}:Mechanism:water_flow",
        ]

    async def _mechanism_alarm_nums(self) -> int:
        count = 0
        for key in self.keys:
            dic = await self.redis_conn.hgetall(key)
            for v in dic.values():
                # print(v)
                if v != "0":
                    count += 1
                    break
        return count

    async def health_score(
        self, MQTTClient: aiomqtt.Client, rGrubbs: float, rLscp: float, rSRA: float
    ) -> None:
        nums = await self._mechanism_alarm_nums()
        score = ANOMALY_SCORE + ALERT_SCORE
        score -= min(
            ALERT_SCORE, PER_ALERT_SCORE * (len(alerts[self.iUnit]["alarms"]) + nums)
        )
        remainder = score - ANOMALY_COL_RATIO

        score_adjustments = dict(
            Grubbs=dict(anom=rGrubbs - GRUBBS_ANOM_COL_RATIO, score=GRUBBS_SCORE),
            Lscp=dict(anom=rLscp - LSCP_ANOM_COL_RATIO, score=LSCP_SCORE),
            SRA=dict(anom=rSRA - SRA_ANOM_COL_RATIO, score=SRA_SCORE),
        )

        for sa in score_adjustments.values():
            diff = sa["anom"]
            if diff > 0:
                score -= sa["score"] * diff / remainder

        data = {
            "timestamp": now[self.iUnit],
            "healthscore": round(score, DECIMALS),
        }
        # print(data)
        if healthQ[self.iUnit].full():
            healthQ[self.iUnit].get()
        healthQ[self.iUnit].put(data)

        result_list = list(healthQ[self.iUnit].queue)
        healthscores = []
        timestamps = []

        for item in result_list:
            healthscores.append(item["healthscore"])
            timestamps.append(item["timestamp"])

        new_data = {"healthscore": healthscores, "timestamp": timestamps}

        await MQTTClient.publish(
            f"{PROJECT_NAME}{self.unit}/Forecast/Health", json.dumps(new_data)
        )


async def test(unit: str) -> None:
    count = 0
    identifier = str(uuid.uuid4())
    tls_params = (
        aiomqtt.TLSParameters(
            ca_certs=MQTT_CA_CERTS,
            certfile=MQTT_CERTFILE,
            keyfile=MQTT_KEYFILE,
            keyfile_password=MQTT_KEYFILE_PASSWORD,
        )
        if MQTT_CA_CERTS
        else None
    )
    try:
        async with aiomqtt.Client(
            hostname=MQTT_IP,
            port=MQTT_PORT,
            username=MQTT_USERNAME,
            password=MQTT_PASSWORD,
            identifier=identifier,
            tls_params=tls_params,
        ) as MQTTClient:
            print("----build new MQTT connection----")
            gd = GetData(unit)
            ad = AnomalyDetection(unit)
            fore = Forecast(unit)
            lgc = Logic(unit)
            hlth = Health(unit)
            while 1:
                start = time()

                gd.get_forecast_df()
                gd.get_anomaly_df()
                fore.vr_forecast()
                fore.ar_forecast1()
                lgc.getNow()

                rGrubbs = ad.grubbs_t()
                rLscp = ad.lscp_t()
                rSRA = ad.spectral_residual_saliency()
                print(rGrubbs, rLscp, rSRA)
                await hlth.health_score(
                    MQTTClient,
                    rGrubbs,
                    rLscp,
                    rSRA,
                )
                await lgc.analysis(MQTTClient)

                end = time()
                elapsed_time = int((end - start) * 1000000)
                count += 1
                print(f"Loop {count} time used: {elapsed_time} microseconds")
                sleep(max(INTERVAL - elapsed_time / 1000000, 0))
    except aiomqtt.MqttError:
        print(f"MQTT connection lost; Reconnecting in 5 seconds ...")
        await asyncio.sleep(5)
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Shutting down the executor...")
        # await redis_conn.aclose()


class Tasks:

    def __init__(self, unit: str) -> None:
        app = AppContainer()
        app.wire(modules=[__name__])

        self.unit = unit
        self.exec = futures.ThreadPoolExecutor()
        self.count = 0

        self.gd = GetData(unit)
        self.ad = AnomalyDetection(unit)
        self.fore = Forecast(unit)
        self.lgc = Logic(unit)
        self.hlth = Health(unit)
        self.identifier = str(uuid.uuid4())
        self.tls_params = (
            aiomqtt.TLSParameters(
                ca_certs=MQTT_CA_CERTS,
                certfile=MQTT_CERTFILE,
                keyfile=MQTT_KEYFILE,
                keyfile_password=MQTT_KEYFILE_PASSWORD,
            )
            if MQTT_CA_CERTS
            else None
        )

    def _get_data(self) -> None:
        future_get_forecast_df = self.exec.submit(self.gd.get_forecast_df)
        future_get_anomaly_df = self.exec.submit(self.gd.get_anomaly_df)
        futures.wait([future_get_forecast_df, future_get_anomaly_df])

    def _anom_fore(self) -> Tuple[float, ...]:
        future_grubbs = self.exec.submit(self.ad.grubbs_t)
        future_lscp = self.exec.submit(self.ad.lscp_t)
        future_spectral = self.exec.submit(self.ad.spectral_residual_saliency)
        future_vr_forecast = self.exec.submit(self.fore.vr_forecast)
        future_ar_forecast = self.exec.submit(self.fore.ar_forecast1)
        future_get_now = self.exec.submit(self.lgc.getNow)
        futures.wait(
            cast(
                Iterable[futures.Future],
                [
                    future_ar_forecast,
                    future_vr_forecast,
                    future_grubbs,
                    future_lscp,
                    future_spectral,
                    future_get_now,
                ],
            )
        )
        return (
            future_grubbs.result(),
            future_lscp.result(),
            future_spectral.result(),
        )

    async def _heal_anal(
        self, anoms: Tuple[float, ...], MQTTClient: aiomqtt.Client
    ) -> None:
        tasks = []
        tasks.append(
            await asyncio.to_thread(self.hlth.health_score, MQTTClient, *anoms)
        )
        tasks.append(await asyncio.to_thread(self.lgc.analysis, MQTTClient))
        await asyncio.gather(*tasks)

    async def _task(self, MQTTClient: aiomqtt.Client) -> None:
        self._get_data()
        anoms = self._anom_fore()
        await asyncio.gather(self._heal_anal(anoms, MQTTClient))

    async def run(self) -> None:
        try:
            async with aiomqtt.Client(
                hostname=MQTT_IP,
                port=MQTT_PORT,
                username=MQTT_USERNAME,
                password=MQTT_PASSWORD,
                identifier=self.identifier,
                tls_params=self.tls_params,
            ) as MQTTClient:
                print("----build new MQTT connection----")

                while 1:
                    start = time()

                    await self._task(MQTTClient)

                    end = time()
                    elapsed_time = int((end - start) * 1000000)
                    self.count += 1
                    print(
                        f"Loop {self.count} (unit {self.unit}) time used: {elapsed_time} microseconds"
                    )
                    sleep(max(INTERVAL - elapsed_time / 1000000, 0))
        except aiomqtt.MqttError:
            print(f"MQTT connection lost; Reconnecting in 5 seconds ...")
            await asyncio.sleep(5)
        except KeyboardInterrupt:
            print("KeyboardInterrupt received. Shutting down the executor...")
            # await redis_conn.aclose()
            self.exec.shutdown(wait=False)


if __name__ == "__main__":
    app = AppContainer()
    app.wire(modules=[__name__])

    asyncio.run(test("2"))
