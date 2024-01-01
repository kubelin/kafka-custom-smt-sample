package com.smt.transformation;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

public class CustomReplaceFileds<T extends ConnectRecord<T>> implements Transformation<T> {

	private Set<String> excludeFields = new HashSet<>();

    interface ConfigName {
        String EXCLUDE = "exclude";
        String INCLUDE = "include";

        // for backwards compatibility
        String INCLUDE_ALIAS = "whitelist";
        String EXCLUDE_ALIAS = "blacklist";
        // for rename field
        String RENAME = "renames";
    }
    

	/** call ConfigDef() **/
	@Override
	public ConfigDef config() {
		return new ConfigDef();
	}

    /**
     * Define a new configuration with no special validation logic
     * @param name          The name of the config parameter
     * @param type          The type of the config
     * @param defaultValue  The default value to use if this config isn't present
     * @param importance    The importance of this config: is this something you will likely need to change.
     * @param documentation The documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
	public static final ConfigDef CONFIG_DEF = new ConfigDef()
		    .define(ConfigName.EXCLUDE, 
		    		ConfigDef.Type.LIST, 
		    		Collections.emptyList(), 
		    		ConfigDef.Importance.MEDIUM,
                    "Fields to exclude. This takes precedence over the fields to include."
		    		)
		    .define(ConfigName.EXCLUDE_ALIAS, 
		    		ConfigDef.Type.LIST, 
		    		null, 
		    		Importance.LOW,
                    "Deprecated. Use " + ConfigName.EXCLUDE + " instead."
                    );
	
	/** Configuration specification for this transformation. **/
	@Override
	public void configure(Map<String, ?> configs) {
		if (configs.containsKey("exclude")) {
			Object excludeConfig = configs.get("exclude");
			if (excludeConfig instanceof String) {
				String[] excludeArray = ((String) excludeConfig).split(",");
				for (String excludeField : excludeArray) {
					excludeFields.add(excludeField.trim());
				}
			}
		}

	}
	
	 /**
     * Apply transformation to the {@code record} and return another record object (which may be {@code record} itself) or {@code null},
     * corresponding to a map or filter operation respectively.
     *
     * A transformation must not mutate objects reachable from the given {@code record}
     * (including, but not limited to, {@link org.apache.kafka.connect.header.Headers Headers},
     * {@link org.apache.kafka.connect.data.Struct Structs}, {@code Lists}, and {@code Maps}).
     * If such objects need to be changed, a new ConnectRecord should be created and returned.
     *
     * The implementation must be thread-safe.
     */
	@Override
	public T apply(T record) {
		if (record.value() instanceof Struct && record.value() != null) {
			
			// if target field in exclude's liet
			Struct value = (Struct) record.value();
			for( String exField : excludeFields ) {
				if (value.schema().field(exField) != null) {
				     // Get the field value based on the schema
	                String fieldValue = value.getString(exField);
	                // Modify the field value
	                String modifiedValue = fieldValue + "+kubelin";
	                value.put(exField, modifiedValue);
				}
			}
		}

		return record;
	}
	
	/** Signal that this transformation instance will no longer will be used. **/	
	@Override
	public void close() {
	}

}
