# Typechecker Comparison for bfb_delivery

This document compares different Python typecheckers tested on the bfb_delivery codebase and provides recommendations.

## Tools Tested

1. **pytype** (existing) - Google's static type analyzer
2. **pyright** - Microsoft's static type checker
3. **mypy** - The standard Python type checker from the Python community
4. **basedpyright** - A fork of pyright with additional features and stricter checking

Note: `ty` and `pyrefly` were considered but not available as installable packages.

## Test Results Summary

### Error Counts (on full codebase: src, tests, docs, scripts)

| Tool | Errors | Warnings | Status |
|------|--------|----------|--------|
| pytype | 0 | 0 | ✅ PASS |
| mypy (with type stubs) | 91 | 0 | ❌ FAIL |
| pyright (with config) | 194 | 0 | ❌ FAIL |
| basedpyright (with config) | 230 | 942 | ❌ FAIL |

### Error Counts (on src only)

| Tool | Errors | Status |
|------|--------|--------|
| pytype | 0 | ✅ PASS |
| mypy | 58 | ❌ FAIL |
| pyright | 79 | ❌ FAIL |

## Detailed Analysis

### pytype (Recommended)
**Pros:**
- Already integrated and passing with zero errors
- Well-suited for codebases using libraries like pandas, openpyxl
- More lenient with dynamic Python patterns
- Good balance of strictness vs. usability

**Cons:**
- Does not support Python 3.13+ (currently disabled for 3.13+)
- Slower than some alternatives
- Less strict than mypy/pyright in some cases

**Errors:** None

### mypy
**Pros:**
- Standard in the Python community
- Good documentation and wide adoption
- Can be configured for varying strictness levels

**Cons:**
- 91 errors on this codebase (58 in src/)
- Requires type stubs for third-party libraries
- Many errors related to pandas DataFrame typing with pandera
- Errors include:
  - Incompatible types with pandera DataFrames
  - numpy.bool vs bool issues
  - openpyxl typing issues
  - Path vs str incompatibilities

**Errors:** Would require substantial code refactoring to fix

### pyright
**Pros:**
- Fast performance
- Integrated with VS Code
- Good type inference
- Active development by Microsoft

**Cons:**
- 194 errors on this codebase (79 in src/)
- Very strict about pandas/DataFrame operations
- Many errors with Series vs DataFrame type inference
- Configuration helps but still has many errors

**Errors:** Would require substantial code refactoring to fix

### basedpyright
**Pros:**
- Even more features than pyright
- Stricter checking can catch more bugs

**Cons:**
- 230 errors + 942 warnings on this codebase
- Too strict for this codebase without extensive refactoring
- Many warnings about "Unknown" types from pandas operations

**Errors:** Would require extensive code refactoring to fix

## Common Error Patterns

Across mypy, pyright, and basedpyright, the most common errors were:

1. **Pandera DataFrame typing**: The codebase uses pandera for DataFrame schema validation. The typecheckers struggle with type narrowing for these typed DataFrames.

2. **Pandas type inference**: Operations like `df['column']` can return Series or DataFrame depending on context, causing type confusion.

3. **openpyxl types**: The library returns union types like `Cell | MergedCell` and `Worksheet | _WorkbookChild` that are difficult to narrow.

4. **numpy.bool vs bool**: Some functions return numpy.bool which is not directly compatible with Python's bool.

5. **Missing type annotations**: Some test helper functions lack type annotations.

## Runtime Comparison

All tools ran quickly enough that runtime is not a major concern:
- pytype: ~90 seconds (full analysis with caching)
- mypy: ~5 seconds
- pyright: ~3 seconds  
- basedpyright: ~3 seconds

## Recommendation

**Recommended Tool: pytype (continue using current setup)**

### Rationale:

1. **Zero errors**: pytype already passes cleanly, meaning the codebase is already type-safe according to pytype's analysis.

2. **Better pandas/numpy support**: pytype handles the complex pandas and numpy typing patterns used in this codebase better than the alternatives.

3. **No code changes required**: Adopting pytype (which is already in use) requires no code refactoring.

4. **Avoiding substantial refactoring**: The issue guidelines state: "If tool disagrees with other installed typecheckers... choose one to disable if necessary." Since pytype passes and the others fail with 58-230 errors, disabling the stricter tools is the right choice.

5. **Python 3.13+ consideration**: While pytype doesn't support Python 3.13+, the codebase currently requires Python 3.12, giving time to evaluate alternatives when upgrading.

### Alternative for Python 3.13+

When Python 3.13+ support is needed:
- **First choice: mypy** - Most mature, configurable, but will require fixing 91 errors
- **Second choice: pyright** - Fast and good inference, but will require fixing 194 errors
- **Not recommended: basedpyright** - Too strict for this codebase (230 errors + 942 warnings)

### Migration Path (if needed in future)

If migrating away from pytype becomes necessary:

1. Choose mypy as the target (fewest errors)
2. Install type stubs: `pandas-stubs`, `types-openpyxl`, `types-requests`
3. Focus on fixing src/ errors first (58 errors)
4. Add type: ignore comments for complex pandas operations that are runtime-validated by pandera
5. Gradually fix test errors
6. Configure mypy to be less strict initially, then incrementally increase strictness

## Configuration Files Created

- `pyrightconfig.json` - Disables most "unknown type" warnings
- `basedpyright.json` - Extends pyrightconfig.json
- `pyproject.toml` - Adds mypy configuration with `ignore_missing_imports = true`

These configs reduced errors/warnings but not enough to make the tools pass.

## Conclusion

Continue using **pytype** as the primary typechecker for this repository. The tool provides good type safety without requiring code changes, and it works well with the pandas/numpy-heavy codebase. Consider mypy for Python 3.13+ migration in the future, but only after allocating time for the required refactoring.
