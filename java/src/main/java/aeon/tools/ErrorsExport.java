package aeon.tools;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.protocol.Errors;

public class ErrorsExport {
    private static String jsonEscape(String input) {
        StringBuilder sb = new StringBuilder(input.length() + 16);
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    sb.append(c);
                    break;
            }
        }
        return sb.toString();
    }

    private static boolean isRetriable(Errors error) {
        ApiException ex = error.exception();
        return ex instanceof RetriableException;
    }

    private static boolean isFatal(Errors error) {
        ApiException ex = error.exception();
        if (ex == null) {
            return false;
        }
        try {
            Class<?> fatalClass = Class.forName("org.apache.kafka.common.errors.FatalException");
            return fatalClass.isInstance(ex);
        } catch (ClassNotFoundException ignored) {
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        String outFile = args.length > 0 ? args[0] : "errors.json";
        Errors[] values = Errors.values();

        StringBuilder sb = new StringBuilder();
        sb.append("[\n");
        for (int i = 0; i < values.length; i++) {
            Errors e = values[i];
            sb.append("  {");
            sb.append("\"name\":\"").append(jsonEscape(e.name())).append("\",");
            sb.append("\"code\":").append(e.code()).append(",");
            sb.append("\"message\":\"").append(jsonEscape(e.message())).append("\",");
            sb.append("\"retriable\":").append(isRetriable(e)).append(",");
            sb.append("\"fatal\":").append(isFatal(e));
            sb.append("}");
            if (i < values.length - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append("]\n");

        Path outPath = Paths.get(outFile);
        Files.writeString(outPath, sb.toString());
        System.out.println("Wrote: " + outPath.toAbsolutePath());
    }
}

