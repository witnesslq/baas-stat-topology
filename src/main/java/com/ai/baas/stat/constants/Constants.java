package com.ai.baas.stat.constants;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by xin on 16-3-21.
 */
public class Constants {
    public static class TupleMappingKey {
        public static final String SERVICE_TYPE = "service_type";
    }
    private static String GroupValue;
    
    public static String getGroupValue() {
		return GroupValue;
	}
	public static List<String> getListStatFields(String fields,String date){
    	List<String> list = new ArrayList<String>();
		if(fields.contains("{")){
			String[] split = fields.split(",");
			for (int i = 0; i < split.length; i++) {
				if(!split[i].contains("{")){
					list.add(split[i]);
				}
			}
			String subs = split[split.length-1];
			String substring = subs.substring(1, subs.length()-1);
			String[] split2 = substring.split(":");
			split2[0] = date;
			StringBuilder stringBuilder = new StringBuilder();
			if(split2[1].contains(".")){
				String substring4 = split2[1].substring(1, split2[1].length()-1);
				String[] split3 = substring4.split(";");
				//把括号去掉
				for (String string : split3) {
					if (string.contains(".")) {                           
						String substring3 = string.substring(1, string.length()-1);
						String[] split4 = substring3.split("\\.");
						int i1 = Integer.parseInt(String.valueOf(split4[0]));
						int i2 = Integer.parseInt(String.valueOf(split4[1]));
						String substring2 = split2[0].substring(i1, i2);
						stringBuilder.append(substring2);
					}
					if(!string.contains(".")&&!string.equals("null")){
						stringBuilder.append(string);
					}
				}
				list.add(stringBuilder.toString());
			}else {
				String substring4 = split2[1].substring(1, split2[1].length()-1);
				String[] split3 = substring4.split(";");
				for (String string : split3){
					if(!string.contains(".")&&!string.equals("null")){
						stringBuilder.append(string);
					}
				}
				list.add(stringBuilder.toString());
				
			}
			//此处是字段的value
			if (split2[2].contains(".")) {
				String subs2 = split2[2].substring(1, split2[2].length()-1);
				String[] split22 = subs2.split("\\.");
				int c1 = Integer.parseInt(String.valueOf(split22[0]));
				int c2 = Integer.parseInt(String.valueOf(split22[1]));
				list.add(split2[0].substring(c1, c2));
				
			} else {
				list.add(split2[2]);
			}
			return list;
		}else {
			return Arrays.asList(fields.split(","));
		}
    }
}
