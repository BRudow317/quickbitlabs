import { defineConfig } from "@hey-api/openapi-ts";

//https://heyapi.dev/openapi-ts/get-started
export default defineConfig({
  input: "http://localhost:8000/openapi.json",
  output: {
    path: "./src/api/openapi",
    postProcess: ["prettier", "eslint"],
  },
  plugins: [
    "@hey-api/client-axios",
    "@hey-api/typescript",
    "@hey-api/schemas",
    // "@hey-api/transformers",
    {
      name: "@hey-api/sdk",
      client: "@hey-api/client-axios",
      operations: {
        strategy: "flat", // Groups into class

        //containerName: "openapi", // The name of the generated SDK object
        methodNameBuilder: (operation: { id?: string; name?: string }) => {
          const baseName = operation.id || operation.name || "operation";
          return baseName.toLowerCase().replace(/\s+/g, "_");
        },
      } as unknown,
    },
  ],
});
