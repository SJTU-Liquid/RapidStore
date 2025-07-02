## Abstraction Layer of Dynamic Graph Storage Libraries

### How to Use

- Import: 

```cmake
ADD_EXECUTABLE(livegraph_wrapper_test wrapper_test.cpp)
TARGET_LINK_LIBRARIES(livegraph_wrapper_test PUBLIC livegraph_wrapper)
```

- Function Call

All functions available are defined in `wrapper.h`

- Notes
  - Based on the version differences in the build toolchain, some header files might need to be included in the libraries.
  - Some libraries might need to be rebuilt to fit the real environment of machine.



### Naming Convention
- Types: `CamelCase`
- Functions & Variables: `snake_case`