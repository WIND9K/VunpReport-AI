"""
config/report_map_ai.py — Report map cho onusreport-ai.

Tái sử dụng cấu trúc từ OnusReport report_map.py, giữ nguyên endpoint/filter/fields.
Thêm thông tin cho Risk + Parquet (schema_name, diary_table, etl_meta).
"""

# ======== Endpoint và Fields — giống hệt OnusReport ========
ENDPOINT_TRANSFERS = "/api/transfers"
ENDPOINT_SPOT = "/api/onus_spot_fund/transactions"

FIELDS_TRANSFERS = (
    "transactionNumber,date,amount,"
    "from.user.id,from.user.display,"
    "to.user.id,to.user.display,"
    "type.internalName"
)

FIELDS_SPOT = (
    "date,transactionNumber,related.user.id,related.user.display,"
    "type.name,amount,currency,description,authorizationStatus"
)


# ======== REPORT_MAP_AI: flat map "group/kind" → spec ========

REPORT_MAP_AI = {
    # --- ONCHAIN (4 sub-reports) ---
    "onchain/vndc_send": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferTypes",
        "type": "vndcacc.vndc_onchain_send",
        "fields": FIELDS_TRANSFERS,
    },
    "onchain/vndc_receive": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferTypes",
        "type": "vndcacc.vndc_onchain_receive",
        "fields": FIELDS_TRANSFERS,
    },
    "onchain/usdt_send": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferFilters",
        "type": "usdtacc.onchain_send",
        "fields": FIELDS_TRANSFERS,
    },
    "onchain/usdt_receive": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferFilters",
        "type": "usdtacc.onchain_receive",
        "fields": FIELDS_TRANSFERS,
    },

    # --- PRO (4 sub-reports) ---
    "pro/vndc_send": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferTypes",
        "type": "vndcacc.vndc_offchain_send_onuspro",
        "fields": FIELDS_TRANSFERS,
    },
    "pro/vndc_receive": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferTypes",
        "type": "vndcacc.vndc_offchain_send_from_onuspro",
        "fields": FIELDS_TRANSFERS,
    },
    "pro/usdt_send": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferFilters",
        "type": "usdtacc.usdt_offchain_send_onuspro",
        "fields": FIELDS_TRANSFERS,
    },
    "pro/usdt_receive": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferFilters",
        "type": "usdtacc.usdt_offchain_send_from_onuspro",
        "fields": FIELDS_TRANSFERS,
    },

    # --- BUYSELL (4 sub-reports) ---
    "buysell/buy_system": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferFilters",
        "type": "vndcacc.buy_via_system",
        "fields": FIELDS_TRANSFERS,
        "extra_params": {"chargedBack": "false"},
    },
    "buysell/buy_partner": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferFilters",
        "type": "vndcacc.buy_via_agency",
        "fields": FIELDS_TRANSFERS,
        "extra_params": {"chargedBack": "false"},
    },
    "buysell/sell_system": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferFilters",
        "type": "vndcacc.sell_via_system",
        "fields": FIELDS_TRANSFERS,
        "extra_params": {"chargedBack": "false"},
    },
    "buysell/sell_partner": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferFilters",
        "type": "vndcacc.sell_via_agency",
        "fields": FIELDS_TRANSFERS,
        "extra_params": {"chargedBack": "false"},
    },

    # --- EXCHANGE (dùng chung endpoint transfers) ---
    "exchange/vndcacc": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferFilters",
        "type": "vndcacc.exchange",
        "fields": FIELDS_TRANSFERS,
    },
    "exchange/usdtacc": {
        "endpoint": ENDPOINT_TRANSFERS,
        "filter_key": "transferFilters",
        "type": "usdtacc.exchange",
        "fields": FIELDS_TRANSFERS,
    },

    # --- SPOT (1 report) ---
    "spot/daily": {
        "endpoint": ENDPOINT_SPOT,
        "fields": FIELDS_SPOT,
        "base_params": {"authorizationStatuses": "authorized"},
    },
}
