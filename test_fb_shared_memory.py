from fb_dataframe import to_flatbuffer, fb_dataframe_head, fb_dataframe_group_by_sum, fb_dataframe_map_numeric_column
from fb_shared_memory import FbSharedMemory
from test_fb_dataframe import generate_random_df


"""
****************************************
* Please do not modify the test cases. *
****************************************
"""


def test_fb_shared_memory_add_df():
    df1 = generate_random_df(10, 10)
    df2 = generate_random_df(10, 10)
    df3 = generate_random_df(10, 10)

    # Add 3 dataframes to shared memory
    fb_shm = FbSharedMemory()
    fb_shm.add_dataframe("df1", df1)
    fb_shm.add_dataframe("df2", df2)
    fb_shm.add_dataframe("df3", df3)

    # The 3 operations should succeed
    fb_shm2 = FbSharedMemory()
    df1_new = fb_shm2.dataframe_head("df1", 10)
    df2_new = fb_shm2.dataframe_group_by_sum("df2", "int_col", "additional_col_0")
    fb_shm2.dataframe_map_numeric_column("df3", "int_col", lambda x: x + 2)
    df3_new = fb_shm2.dataframe_head("df3", 10)

    fb_shm2.close()

    assert df1_new.equals(df1)
    assert df2_new.equals(df2.groupby("int_col").agg({'additional_col_0': 'sum'}))

    df3["int_col"] = df3["int_col"].apply(lambda x: x + 2)
    assert df3_new.equals(df3)
