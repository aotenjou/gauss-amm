set(TGT_ut_amm_epoch_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/ut_amm_epoch.cpp
        )

include_directories(${PROJECT_SRC_DIR}/include)

add_executable(ut_amm_epoch_opengauss ${TGT_ut_amm_epoch_SRC})
target_compile_options(ut_amm_epoch_opengauss PRIVATE ${OPTIMIZE_LEVEL})
target_link_options(ut_amm_epoch_opengauss PRIVATE ${UNIT_TEST_LINK_OPTIONS_LIB_LIST})
target_link_libraries(ut_amm_epoch_opengauss ${UNIT_TEST_BASE_LIB_LIST})

add_custom_command(TARGET ut_amm_epoch_opengauss
        POST_BUILD
        COMMAND mkdir -p ${CMAKE_BINARY_DIR}/ut_bin
        COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/ut_amm_epoch_opengauss ${CMAKE_BINARY_DIR}/ut_bin/ut_amm_epoch_opengauss
        COMMAND chmod +x ${CMAKE_BINARY_DIR}/ut_bin/ut_amm_epoch_opengauss
        )

add_custom_target(ut_amm_epoch_test
        DEPENDS ut_amm_epoch_opengauss
        COMMAND ${CMAKE_BINARY_DIR}/ut_bin/ut_amm_epoch_opengauss
        COMMENT "run GS AMM epoch saturation unit tests"
        )

set(TGT_ut_ammgranule_release_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/ut_ammgranule_release.cpp
        ${PROJECT_SRC_DIR}/common/backend/utils/mmgr/ammgranule.cpp
        )

add_executable(ut_ammgranule_release_opengauss ${TGT_ut_ammgranule_release_SRC})
target_compile_options(ut_ammgranule_release_opengauss PRIVATE ${OPTIMIZE_LEVEL})
target_link_options(ut_ammgranule_release_opengauss PRIVATE ${UNIT_TEST_LINK_OPTIONS_LIB_LIST})
target_link_libraries(ut_ammgranule_release_opengauss ${UNIT_TEST_BASE_LIB_LIST})

add_custom_command(TARGET ut_ammgranule_release_opengauss
        POST_BUILD
        COMMAND mkdir -p ${CMAKE_BINARY_DIR}/ut_bin
        COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/ut_ammgranule_release_opengauss ${CMAKE_BINARY_DIR}/ut_bin/ut_ammgranule_release_opengauss
        COMMAND chmod +x ${CMAKE_BINARY_DIR}/ut_bin/ut_ammgranule_release_opengauss
        )

add_custom_target(ut_ammgranule_release_test
        DEPENDS ut_ammgranule_release_opengauss
        COMMAND ${CMAKE_BINARY_DIR}/ut_bin/ut_ammgranule_release_opengauss
        COMMENT "run AMM granule free-chunk release unit tests"
        )
