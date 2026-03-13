package MyJavaUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.JsonNode;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Servlet-related helper utilities extracted from JavaHelpers.
 */
public final class ServletUtil {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private ServletUtil() {}

    // Logging and request parsing
    public static void logRequest(HttpServletRequest request, Consumer<String> logger) {
        Consumer<String> log = logger != null ? logger : System.out::println;
        StringBuilder sb = new StringBuilder();

        sb.append("\n╔══════════════════════════════════════════════════════════════╗\n");
        sb.append("║                    HTTP REQUEST DETAILS                       ║\n");
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");

        sb.append(String.format("║ Method:      %-48s ║%n", request.getMethod()));
        sb.append(String.format("║ URI:         %-48s ║%n", truncate(request.getRequestURI(), 48)));
        sb.append(String.format("║ URL:         %-48s ║%n", truncate(request.getRequestURL().toString(), 48)));
        sb.append(String.format("║ Query:       %-48s ║%n", truncate(request.getQueryString(), 48)));
        sb.append(String.format("║ Protocol:    %-48s ║%n", request.getProtocol()));
        sb.append(String.format("║ Remote Addr: %-48s ║%n", request.getRemoteAddr()));
        sb.append(String.format("║ Content-Type:%-48s ║%n", request.getContentType()));

        sb.append("╠══════════════════════════════════════════════════════════════╣\n");
        sb.append("║ HEADERS:                                                     ║\n");
        Collections.list(request.getHeaderNames()).forEach(name ->
            sb.append(String.format("║   %-15s: %-42s ║%n",
                truncate(name, 15), truncate(request.getHeader(name), 42))));

        sb.append("╠══════════════════════════════════════════════════════════════╣\n");
        sb.append("║ PARAMETERS:                                                  ║\n");
        request.getParameterMap().forEach((name, values) ->
            sb.append(String.format("║   %-15s: %-42s ║%n",
                truncate(name, 15), truncate(String.join(", ", values), 42))));

        if (request.getCookies() != null) {
            sb.append("╠══════════════════════════════════════════════════════════════╣\n");
            sb.append("║ COOKIES:                                                     ║\n");
            for (Cookie cookie : request.getCookies()) {
                sb.append(String.format("║   %-15s: %-42s ║%n",
                    truncate(cookie.getName(), 15), truncate(cookie.getValue(), 42)));
            }
        }

        sb.append("╚══════════════════════════════════════════════════════════════╝");
        log.accept(sb.toString());
    }

    public static void logRequest(HttpServletRequest request) {
        logRequest(request, null);
    }

    public static String getRequestBody(HttpServletRequest request) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = request.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
        }
        return sb.toString().trim();
    }

    public static <T> T getRequestBodyAs(HttpServletRequest request, Class<T> clazz) throws IOException {
        return JSON_MAPPER.readValue(getRequestBody(request), clazz);
    }

    public static Map<String, String> getRequestParams(HttpServletRequest request) {
        return request.getParameterMap().entrySet().stream()
                .collect(LinkedHashMap::new,
                    (map, entry) -> map.put(entry.getKey(), entry.getValue().length > 0 ? entry.getValue()[0] : ""),
                    Map::putAll);
    }

    public static Map<String, String> getRequestHeaders(HttpServletRequest request) {
        Map<String, String> headers = new LinkedHashMap<>();
        Collections.list(request.getHeaderNames())
                .forEach(name -> headers.put(name, request.getHeader(name)));
        return headers;
    }

    public static String getClientIp(HttpServletRequest request) {
        String[] headerNames = {
            "X-Forwarded-For",
            "X-Real-IP",
            "Proxy-Client-IP",
            "WL-Proxy-Client-IP",
            "HTTP_X_FORWARDED_FOR",
            "HTTP_CLIENT_IP"
        };

        for (String header : headerNames) {
            String ip = request.getHeader(header);
            if (ip != null && !ip.isEmpty() && !"unknown".equalsIgnoreCase(ip)) {
                return ip.split(",")[0].trim();
            }
        }
        return request.getRemoteAddr();
    }

    public static String getBearerToken(HttpServletRequest request) {
        String authHeader = request.getHeader("Authorization");
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            return authHeader.substring(7);
        }
        return null;
    }

    // Response helpers
    public static void sendJson(HttpServletResponse response, Object data, int status)
            throws IOException {
        response.setStatus(status);
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        JSON_MAPPER.writeValue(response.getOutputStream(), data);
    }

    public static void sendJson(HttpServletResponse response, Object data) throws IOException {
        sendJson(response, data, HttpServletResponse.SC_OK);
    }

    public static void sendError(HttpServletResponse response, int status,
            String message, String details) throws IOException {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("timestamp", java.time.Instant.now().toString());
        error.put("status", status);
        error.put("error", message);
        if (details != null) {
            error.put("details", details);
        }
        sendJson(response, error, status);
    }

    public static void sendError(HttpServletResponse response, int status, String message)
            throws IOException {
        sendError(response, status, message, null);
    }

    public static void sendSuccess(HttpServletResponse response, String message, Object data)
            throws IOException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", true);
        result.put("message", message);
        if (data != null) {
            result.put("data", data);
        }
        sendJson(response, result, HttpServletResponse.SC_OK);
    }

    public static void sendSuccess(HttpServletResponse response, String message) throws IOException {
        sendSuccess(response, message, null);
    }

    public static void sendFile(HttpServletResponse response, File file, String filename)
            throws IOException {
        String downloadName = filename != null ? filename : file.getName();
        response.setContentType(Files.probeContentType(file.toPath()));
        response.setContentLengthLong(file.length());
        response.setHeader("Content-Disposition",
            "attachment; filename=\"" + downloadName + "\"");

        try (InputStream in = new FileInputStream(file);
             OutputStream out = response.getOutputStream()) {
            in.transferTo(out);
        }
    }

    public static void sendHtml(HttpServletResponse response, String html, int status)
            throws IOException {
        response.setStatus(status);
        response.setContentType("text/html");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write(html);
    }

    public static void sendRedirect(HttpServletRequest request, HttpServletResponse response,
            String url, String flash) throws IOException {
        if (flash != null) {
            HttpSession session = request.getSession();
            session.setAttribute("_flash", flash);
        }
        response.sendRedirect(url);
    }

    public static void setCorsHeaders(HttpServletResponse response,
            String allowedOrigin, String allowedMethods) {
        response.setHeader("Access-Control-Allow-Origin", allowedOrigin);
        response.setHeader("Access-Control-Allow-Methods", allowedMethods);
        response.setHeader("Access-Control-Allow-Headers",
            "Content-Type, Authorization, X-Requested-With");
        response.setHeader("Access-Control-Allow-Credentials", "true");
        response.setHeader("Access-Control-Max-Age", "3600");
    }

    // Basic helpers
    public static ObjectNode createJsonObject() {
        return JSON_MAPPER.createObjectNode();
    }

    public static ArrayNode createJsonArray() {
        return JSON_MAPPER.createArrayNode();
    }

    public static JsonNode jsonNode(String json) {
        try {
            return JSON_MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON parsing failed", e);
        }
    }

    public static String toJson(Object obj) {
        try {
            return JSON_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON serialization failed", e);
        }
    }

    private static String truncate(String str, int maxLength) {
        if (str == null) return "null";
        return str.length() > maxLength ? str.substring(0, maxLength - 3) + "..." : str;
    }
}
