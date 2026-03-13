package MyJavaUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.moandjiezana.toml.Toml;
import com.moandjiezana.toml.TomlWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class FormatConverter {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
            new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
    private static final XmlMapper XML_MAPPER = new XmlMapper();

    private FormatConverter() {}

    public static String jsonToXml(String json, String rootName) {
        try {
            JsonNode jsonNode = JSON_MAPPER.readTree(json);
            return XML_MAPPER.writer()
                .withRootName(rootName)
                .writeValueAsString(jsonNode);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON to XML conversion failed", e);
        }
    }

    public static String xmlToJson(String xml) {
        try {
            JsonNode node = XML_MAPPER.readTree(xml.getBytes(StandardCharsets.UTF_8));
            return JSON_MAPPER.writeValueAsString(node);
        } catch (Exception e) {
            throw new RuntimeException("XML to JSON conversion failed", e);
        }
    }

    public static String jsonToYaml(String json) {
        try {
            JsonNode node = JSON_MAPPER.readTree(json);
            return YAML_MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON to YAML conversion failed", e);
        }
    }

    public static String yamlToJson(String yaml) {
        try {
            JsonNode node = YAML_MAPPER.readTree(yaml);
            return JSON_MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("YAML to JSON conversion failed", e);
        }
    }

    public static String tomlToJson(String toml) {
        try {
            Toml tomlObj = new Toml().read(toml);
            Map<String, Object> map = tomlObj.toMap();
            return JSON_MAPPER.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("TOML to JSON conversion failed", e);
        }
    }

    public static String jsonToToml(String json) {
        try {
            Map<String, Object> map = JSON_MAPPER.readValue(json, Map.class);
            TomlWriter writer = new TomlWriter();
            return writer.write(map);
        } catch (Exception e) {
            throw new RuntimeException("JSON to TOML conversion failed", e);
        }
    }

    public static String convertFormat(String content, String fromFormat, String toFormat) {
        String json;
        switch (fromFormat.toLowerCase()) {
            case "json":
                json = content;
                break;
            case "xml":
                json = xmlToJson(content);
                break;
            case "yaml":
            case "yml":
                json = yamlToJson(content);
                break;
            case "toml":
                json = tomlToJson(content);
                break;
            default:
                throw new IllegalArgumentException("Unsupported source format: " + fromFormat);
        }

        switch (toFormat.toLowerCase()) {
            case "json":
                return toJsonPretty(jsonToMap(json));
            case "xml":
                return jsonToXml(json, "root");
            case "yaml":
            case "yml":
                return jsonToYaml(json);
            case "toml":
                return jsonToToml(json);
            default:
                throw new IllegalArgumentException("Unsupported target format: " + toFormat);
        }
    }

    private static Map<String, Object> jsonToMap(String json) {
        try {
            return JSON_MAPPER.readValue(json, Map.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON parsing failed", e);
        }
    }

    private static String toJsonPretty(Object obj) {
        try {
            return JSON_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON serialization failed", e);
        }
    }
}
