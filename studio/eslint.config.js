import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  js.configs.recommended,
  ...tseslint.configs.strict,
  ...tseslint.configs.stylistic,
  {
    ignores: ["dist", "coverage", "node_modules"],
  },
  {
    files: ["**/*.{ts,tsx}"],
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      "@typescript-eslint/consistent-type-definitions": ["error", "type"],
      "@typescript-eslint/no-misused-promises": [
        "error",
        {
          checksVoidReturn: {
            attributes: false,
          },
        },
      ],
      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
        },
      ],
      "@typescript-eslint/no-non-null-assertion": "off",
    },
  },
);
