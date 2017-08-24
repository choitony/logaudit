package corp.qihoo.bigaudit.log;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by chaiwentao on 2017/8/11.
 */
public class HdfsNameLog implements LogInterface<String, String, Map<String, String>>, Serializable {

    public final static Logger logger = LoggerFactory.getLogger(HdfsNameLog.class);

    private ParameterTool properties;
    // private String[] fields;

    protected Pattern pattern;
    private String regex = "^((((1[6-9]|[2-9]\\d)\\d{2})-(0?[13578]|1[02])-(0?[1-9]|[12]\\d|3[01]))|(((1[6-9]|[2-9]\\d)\\d{2})-(0?[13456789]|1[012])-(0?[1-9]|[12]\\d|30))|(((1[6-9]|[2-9]\\d)\\d{2})-0?2-(0?[1-9]|1\\d|2[0-8]))|(((1[6-9]|[2-9]\\d)(0[48]|[2468][048]|[13579][26])|((16|[2468][048]|[3579][26])00))-0?2-29-))\\s(((0|1)[0-9])|(2[0-3])):([0-5][0-9]):([0-5][0-9])";
    //:\\d+{5}";
    protected String ipRegex = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public HdfsNameLog(ParameterTool properties) {
        this.properties = properties;
        pattern = Pattern.compile(this.regex);
    }

    public FilterFunction<String> getFilterFunction() {
        FilterFunction<String> filterFunction = new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (value.indexOf("org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit") != -1) {
                    return true;
                } else
                    return false;
            }
        };
        return filterFunction;
    }

    public FlatMapFunction<String, String> getFlatMapFunction() {

        FlatMapFunction<String, String> flatmapFunction = new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String logs, Collector<String> out) {
                String[] lines = logs.split("\n");
                for (String line : lines) {
                    out.collect(line);
                }
            }
        };
        return flatmapFunction;
    }

    public MapFunction<String, Map<String, String>> getMapFunction() {

        MapFunction<String, Map<String, String>> mapFunction = new MapFunction<String, Map<String, String>>() {
            @Override
            public Map<String, String> map(String value) {
                //logger.info("value:" + value);
                Map<String, String> json = new HashMap<String, String>();
                try {
                    Map<String, String> ret = process(value, "=", null);
                    String user = ret.get("ugi");
                    //把用户和用户组分开
                    String[] userGroup = user.split(",", 2);
                    if (userGroup.length > 1) {
                        user = userGroup[0];
                        ret.put("user_group", userGroup[1]);
                        ret.put("user", userGroup[0]);
                    }
                    ret.remove("ugi");
                    json.put("qihoo_audit_timestamp", getLogTime(value));
                    json.put("qihoo_audit_cluster_type", "hdfs");
                    json.put("qihoo_audit_cluster_name", "dfs.shbt");
                    json.put("qihoo_audit_user", ret.get("user"));
                    json.put("qihoo_audit_operation", ret.get("cmd"));
                    String ip = ret.get("ip");
                    if (ip.contains("/")) {
                        ip = ip.replace("/", "");
                    }
                    json.put("qihoo_audit_ip", ip);
                    ret.remove("ip");
                    String uri = ret.get("src");
                    ret.remove("src");
                    json.put("qihoo_audit_uri", uri);
                    json.put("qihoo_audit_policy", "all");
                    json.put("qihoo_audit_logtype", null);
                    json.put("qihoo_audit_ret", value);
                } catch (Exception ex) {
                    System.out.println(value);
                }
                return json;
            }
        };
        return mapFunction;
    }

    protected Map<String, String> process(String input, String split, String content) throws Exception {
        Map<String, String> ret = new HashMap<String, String>();
        if (input == null || input.length() == 0) {
            return null;
        }
        String[] log = input.split(" ", 5);
        ret.put("time", log[0]);
        String[] feilds = log[4].split("\t");
        for (String feild : feilds) {
            String[] tt = feild.split("=");
            ret.put(tt[0].trim(), tt[1].trim());
        }
        return ret;
    }

    protected String getLogTime(String input) {
        Matcher match = pattern.matcher(input);
        String dateTimeStr = null;
        if (match.find()) {
            dateTimeStr = match.group(0);
        } else {
            dateTimeStr = sdf.format(new Date());
        }
        return dateTimeStr;
    }
}
