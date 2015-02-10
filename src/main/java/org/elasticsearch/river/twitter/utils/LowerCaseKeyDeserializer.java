package org.elasticsearch.river.twitter.utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.KeyDeserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.Version;

public class LowerCaseKeyDeserializer extends KeyDeserializer {
	@Override
	public Object deserializeKey(String key, DeserializationContext ctx)
			throws IOException, JsonProcessingException {
		return key.toLowerCase();
	}

	@SuppressWarnings("unchecked")
	public static String formatToES(String json) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		SimpleModule module = new SimpleModule("LowerCaseKeyDeserializer",
				new Version(1, 0, 0, null));
		module.addKeyDeserializer(Object.class, new LowerCaseKeyDeserializer());
		mapper.registerModule(module);
		Map<String, Object> map = (Map<String, Object>) mapper.readValue(
				json, Map.class);
		toLowerMap(map);
		return mapper.writeValueAsString(map);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void toLowerMap(Map<String, Object> map){
		Set<String> keys = new HashSet<String>(map.keySet());
		
		for(String key: keys){
			Object remove = map.remove(key);
			if(remove == null || (remove instanceof String && ((String) remove).isEmpty()))
				continue;
			map.put(key.toLowerCase(), remove);
			if(remove instanceof Map) {
				Map<String, Object> childMap = (Map<String, Object>) remove;
				toLowerMap(childMap);
				if(childMap.isEmpty())
					map.remove(key.toLowerCase());
			}
			
			if(remove instanceof List){
				List list = (List) remove;
				for(Object o: list){
					if(o instanceof Map) {
						Map<String, Object> childMap = (Map<String, Object>) o;
						toLowerMap(childMap);
					}
				}
			}
		}
	}
}
