package org.lxk.gmall.realtime.common;

public class GmallConstant {
    public static final String KAFAK_BROCKERS = "hadoop162:9092,hadoop163:9092,hadoop164:9092";

    public static final String TOP_ODS_DB = "ods_db";
    public static final String MYSQL_HOSTNAME = "hadoop162";
    public static final int MYSQL_PORT = 3306;
    public static final String CONFIG_DATABASE = "gmall2023_config";
    public static final String MYSQL_USERNAME = "root";
    public static final String MYSQ_PASSWD = "aaaaaa";
    public static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String JDB_URL = "jdbc:mysql://hadoop162:3306?useSSL=false";
    public static final String ODS_DB = "ods_db";

    public static final String ODS_LOG = "ods_log";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String ZOOKEEPER_BOOTSTRAPER = "hadoop162:2181,hadoop163:2181";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_CANCEL_ORDER_DETAIL = "dwd_trade_cancel_order_detail";
    public static final String TOPIC_DWD_TRADE_PAY_DETAIL_SUC = "dwd_trade_pay_detail_suc";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
}
