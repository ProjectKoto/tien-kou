import js from "@eslint/js"
import { globalIgnores } from "eslint/config"
import tseslint from "typescript-eslint"
import globals from "globals"
import stylistic from '@stylistic/eslint-plugin'

export default tseslint.config([
  globalIgnores(["./thirdparty/*"]),
  {
    languageOptions: {
      globals: {
        ...globals.browser,
        ...globals.node,
      },
    },
  },
  { files: ["**/*.{js,mjs,cjs,ts,mts,cts}"], plugins: { js }, extends: [ js.configs.recommended ] },
  tseslint.configs.recommended,
  {
    rules: {
      "no-empty-pattern": "warn",
      "@typescript-eslint/no-explicit-any": "warn",
      "@typescript-eslint/no-unused-vars": ["warn", {
        "vars": "all",
        "args": "after-used",
        "caughtErrors": "all",
        "ignoreRestSiblings": false,
        "reportUsedIgnorePattern": false,
        "varsIgnorePattern": "(^_)|(^a$)|(^b$)|(^c$)",
        "argsIgnorePattern": "(^_)|(^a$)|(^b$)|(^c$)",
        "caughtErrorsIgnorePattern": "(^_)|(^a$)|(^b$)|(^c$)",
      }],
      "no-unused-vars": ["off" /* , {
        "vars": "all",
        "args": "after-used",
        "caughtErrors": "all",
        "ignoreRestSiblings": false,
        "reportUsedIgnorePattern": false,
        "varsIgnorePattern": "(^_)|(^a$)|(^b$)|(^c$)",
        "argsIgnorePattern": "(^_)|(^a$)|(^b$)|(^c$)",
      } */],

    }
  },
  {
    plugins: {
      '@stylistic': stylistic,
    },
    rules: {
      '@stylistic/indent': ['error', 2],
      '@stylistic/semi': ['error', 'never'],
    },
  }
])
