import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class MyFileUtil {
    // FILE HANDLING HELPERS
    

    /**
     * readFile - Read entire file to string
     * 
     * @param path File path
     * @return File contents
     * 
     * @example
     * String content = readFile("config.json");
     * String content = readFile(Paths.get("/etc/app/config.yaml"));
     */
    public static String readFile(Path path) throws IOException {
        return Files.readString(path, StandardCharsets.UTF_8);
    }
    
    public static String readFile(String path) throws IOException {
        return readFile(Paths.get(path));
    }

    /**
     * readFileLines - Read file as list of lines
     * 
     * @param path File path
     * @return List of lines
     * 
     * @example
     * List<String> lines = readFileLines("data.txt");
     */
    public static List<String> readFileLines(Path path) throws IOException {
        return Files.readAllLines(path, StandardCharsets.UTF_8);
    }
    
    public static List<String> readFileLines(String path) throws IOException {
        return readFileLines(Paths.get(path));
    }

    /**
     * writeFile - Write string to file
     * 
     * @param path    File path
     * @param content Content to write
     * @param append  Whether to append (false = overwrite)
     * 
     * @example
     * writeFile("output.txt", "Hello World", false);
     * writeFile("log.txt", logLine + "\n", true);
     */
    public static void writeFile(Path path, String content, boolean append) throws IOException {
        if (append) {
            Files.writeString(path, content, StandardCharsets.UTF_8, 
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } else {
            Files.writeString(path, content, StandardCharsets.UTF_8);
        }
    }
    
    public static void writeFile(String path, String content, boolean append) throws IOException {
        writeFile(Paths.get(path), content, append);
    }
    
    public static void writeFile(String path, String content) throws IOException {
        writeFile(path, content, false);
    }

    /**
     * copyFile - Copy file with options
     * 
     * @param source      Source file path
     * @param destination Destination path
     * @param overwrite   Whether to overwrite existing
     * 
     * @example
     * copyFile("source.txt", "backup/source.txt", true);
     */
    public static void copyFile(Path source, Path destination, boolean overwrite) throws IOException {
        if (overwrite) {
            Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
        } else {
            Files.copy(source, destination);
        }
    }
    
    public static void copyFile(String source, String destination, boolean overwrite) throws IOException {
        copyFile(Paths.get(source), Paths.get(destination), overwrite);
    }

    /**
     * moveFile - Move or rename file
     * 
     * @param source      Source path
     * @param destination Destination path
     * 
     * @example
     * moveFile("temp/upload.tmp", "files/document.pdf");
     */
    public static void moveFile(Path source, Path destination) throws IOException {
        Files.createDirectories(destination.getParent());
        Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
    }
    
    public static void moveFile(String source, String destination) throws IOException {
        moveFile(Paths.get(source), Paths.get(destination));
    }

    /**
     * deleteFile - Delete file or directory recursively
     * 
     * @param path Path to delete
     * 
     * @example
     * deleteFile("temp/cache");
     * deleteFile("old-file.txt");
     */
    public static void deleteFile(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) 
                        throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) 
                        throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            Files.deleteIfExists(path);
        }
    }
    
    public static void deleteFile(String path) throws IOException {
        deleteFile(Paths.get(path));
    }

    /**
     * listFiles - List files in directory with optional filter
     * 
     * @param directory Directory path
     * @param extension Optional file extension filter (e.g., ".txt")
     * @param recursive Whether to search recursively
     * @return List of file paths
     * 
     * @example
     * List<Path> txtFiles = listFiles("documents", ".txt", true);
     * List<Path> allFiles = listFiles("uploads", null, false);
     */
    public static List<Path> listFiles(Path directory, String extension, boolean recursive) 
            throws IOException {
        try (Stream<Path> stream = recursive 
                ? Files.walk(directory) 
                : Files.list(directory)) {
            return stream
                .filter(Files::isRegularFile)
                .filter(p -> extension == null || p.toString().endsWith(extension))
                .collect(Collectors.toList());
        }
    }
    
    public static List<Path> listFiles(String directory, String extension, boolean recursive) 
            throws IOException {
        return listFiles(Paths.get(directory), extension, recursive);
    }

    public static String getFileExtension(String filename) {
        int lastDot = filename.lastIndexOf('.');
        return lastDot > 0 ? filename.substring(lastDot + 1) : "";
    }

    public static String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "";
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }

    /**
     * getFileInfo - Get detailed file information
     * 
     * @param path File path
     * @return Map with file details
     * 
     * @example
     * Map<String, Object> info = getFileInfo("document.pdf");
     * // { name, size, sizeFormatted, extension, lastModified, ... }
     */
    public static Map<String, Object> getFileInfo(Path path) throws IOException {
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
        
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name", path.getFileName().toString());
        info.put("path", path.toAbsolutePath().toString());
        info.put("size", attrs.size());
        info.put("sizeFormatted", formatFileSize(attrs.size()));
        info.put("extension", getFileExtension(path.toString()));
        info.put("lastModified", attrs.lastModifiedTime().toInstant().toString());
        info.put("created", attrs.creationTime().toInstant().toString());
        info.put("isDirectory", attrs.isDirectory());
        info.put("isSymbolicLink", attrs.isSymbolicLink());
        info.put("mimeType", Files.probeContentType(path));
        
        return info;
    }


    /**
     * zipFiles - Create ZIP archive from files
     * 
     * @param zipPath Output ZIP file path
     * @param files   Files to include
     * 
     * @example
     * zipFiles("archive.zip", List.of(
     *     Paths.get("file1.txt"),
     *     Paths.get("file2.txt")
     * ));
     */
    public static void zipFiles(Path zipPath, List<Path> files) throws IOException {
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipPath.toFile()))) {
            for (Path file : files) {
                ZipEntry entry = new ZipEntry(file.getFileName().toString());
                zos.putNextEntry(entry);
                Files.copy(file, zos);
                zos.closeEntry();
            }
        }
    }

    /**
     * watchDirectory - Watch directory for changes
     * 
     * @param directory Directory to watch
     * @param handler   Callback for file events
     * @return WatchService (caller should close when done)
     * 
     * @example
     * WatchService watcher = watchDirectory("uploads", event -> {
     *     System.out.println(event.kind() + ": " + event.context());
     * });
     */
    public static WatchService watchDirectory(Path directory, 
            Consumer<WatchEvent<?>> handler) throws IOException {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        directory.register(watchService,
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_DELETE,
            StandardWatchEventKinds.ENTRY_MODIFY);
        
        Thread watchThread = new Thread(() -> {
            try {
                WatchKey key;
                while ((key = watchService.take()) != null) {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        handler.accept(event);
                    }
                    key.reset();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        watchThread.setDaemon(true);
        watchThread.start();
        
        return watchService;
    }
}
