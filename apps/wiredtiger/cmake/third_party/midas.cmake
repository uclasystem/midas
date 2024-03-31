if(NOT HAVE_LIBMIDAS)
    # We can't construct a midas library target.
    return()
endif()

if (NOT ENABLE_MIDAS)
  # We don't want to construct a midas library target.
  return()
endif()

if(TARGET wt::midas)
    # Avoid redefining the imported library.
    return()
endif()

# Define the imported midas library target that can be subsequently linked across the build system.
# We use the double colons (::) as a convention to tell CMake that the target name is associated
# with an IMPORTED target (which allows CMake to issue a diagnostic message if the library wasn't found).
add_library(wt::midas SHARED IMPORTED GLOBAL)
set_target_properties(wt::midas PROPERTIES
    IMPORTED_LOCATION ${HAVE_LIBMIDAS}
)
if (HAVE_LIBMIDAS_INCLUDES)
    set_target_properties(wt::midas PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${HAVE_LIBMIDAS_INCLUDES}
    )
endif()

target_link_libraries(wt::midas INTERFACE "-lstdc++ -lrt -ldl -pthread")