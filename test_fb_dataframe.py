import dill
import flatbuffers
import pandas as pd
import random
import string
import struct
import time
import types


from fb_dataframe import to_flatbuffer, fb_dataframe_head, fb_dataframe_group_by_sum, fb_dataframe_map_numeric_column

"""
****************************************
* Please do not modify the test cases. *
****************************************
"""


def generate_random_df(num_rows: int = 100000, additional_cols: int = 100):
    """
        Generates a random dataframe consisting of an int, float, and string cols, and
        additional_cols number of additional int cols.
    """
    df = pd.DataFrame()

    int_col = list(random.randint(0, 10) for i in range(num_rows))
    float_col = list(random.uniform(0, 10000) for i in range(num_rows))
    string_col = list(''.join(random.choice(string.ascii_uppercase) for _ in range(10)) for i in range(num_rows))

    additional_int_cols = []
    for i in range(additional_cols):
        additional_int_cols.append(list(random.randint(0, 1000) for i in range(num_rows)))

    df["int_col"] = int_col
    df["float_col"] = float_col
    df["string_col"] = string_col

    for i in range(len(additional_int_cols)):
        df[f"additional_col_{i}"] = additional_int_cols[i]

    return df


def test_to_flatbuffer():
    df = generate_random_df(1000, 10)

    fb_df = to_flatbuffer(df)

    int_bytes = bytearray()
    for val in df["int_col"]:
        int_bytes.extend(val.to_bytes(8, "little"))

    float_bytes = bytearray()
    for val in df["float_col"]:
        float_bytes.extend(struct.pack('d', val))

    # The int and float columns should be encoded contiguously
    assert int_bytes in fb_df
    assert float_bytes in fb_df

    # The int column should come before the float column
    assert fb_df.find(int_bytes) < fb_df.find(float_bytes)

    # The utf-8 encoded strings should be in the flatbuffer.
    for val in df["string_col"][:100]:
        assert val.encode('utf-8') in fb_df


def test_fb_dataframe_head_correctness():
    df = generate_random_df(num_rows = 3)

    fb_df = to_flatbuffer(df)

    # Get the head from the flatbuffer; as there are less than 5 rows, all 3 are returned.
    df_head_fb = fb_dataframe_head(fb_df)

    # Compare with the result from dill for ground truth.
    df_dill_bytestring = dill.dumps(df)
    df_deserialized = dill.loads(df_dill_bytestring)
    df_head_dill = df_deserialized.head()

    assert df_head_fb.equals(df_head_dill)


def test_fb_dataframe_head_efficiency():
    df = generate_random_df()

    fb_df = to_flatbuffer(df)

    # Time directly reading dataframe head in flatbuffer.
    start = time.time()
    df_head_fb = fb_dataframe_head(fb_df)
    fb_dataframe_head_time = time.time() - start

    df_dill_bytestring = dill.dumps(df)

    # Time deserializing the dataframe with dill, then reading the head.
    start = time.time()
    df_deserialized = dill.loads(df_dill_bytestring)
    df_head_dill = df_deserialized.head()
    dill_load_time = time.time() - start

    # The flatbuffer implementation should be faster.
    assert df_head_fb.equals(df_head_dill)
    assert fb_dataframe_head_time < dill_load_time, "Your implementation should be faster than deserializing the entire dataframe with dill, then calling head()."


def test_fb_dataframe_group_by_sum():
    df = generate_random_df(num_rows = 100, additional_cols = 3000)

    fb_df = to_flatbuffer(df)

    # Time directly performing groupby flatbuffer.
    start = time.time()
    df_groupby_fb = fb_dataframe_group_by_sum(fb_df, "int_col", "additional_col_0")
    fb_dataframe_groupby_time = time.time() - start

    df_dill_bytestring = dill.dumps(df)

    # Time deserializing the dataframe with dill, then performing the groupby.
    start = time.time()
    df_deserialized = dill.loads(df_dill_bytestring)
    df_groupby_dill = df_deserialized.groupby("int_col").agg({'additional_col_0': 'sum'})
    dill_groupby_time = time.time() - start

    # The flatbuffer implementation should be faster.
    assert df_groupby_fb.equals(df_groupby_dill)
    assert fb_dataframe_groupby_time < dill_groupby_time, "Your implementation should be faster than deserializing the entire dataframe with dill, then doing the groupby."


def test_fb_dataframe_map_numeric_column_correctness():
    df = generate_random_df(num_rows = 10)

    fb_df = to_flatbuffer(df)

    # Map the int column in the dataframe.
    fb_dataframe_map_numeric_column(fb_df, "int_col", lambda x: x * 2)

    # Map the float column in the dataframe.
    fb_dataframe_map_numeric_column(fb_df, "float_col", lambda x: x / 2)

    # Map the string column in the dataframe (this shouldn't do anything).
    fb_dataframe_map_numeric_column(fb_df, "string_col", lambda x: x + 2)

    df2 = fb_dataframe_head(fb_df, len(fb_df))

    df["int_col"] = df["int_col"].apply(lambda x: x * 2)
    df["float_col"] = df["float_col"].apply(lambda x: x / 2)

    assert df2.equals(df)


def test_fb_dataframe_map_numeric_column_efficiency():
    df = generate_random_df(num_rows = 100, additional_cols = 1000)

    fb_df = to_flatbuffer(df)

    # Time performing the operations with flatbuffer map.
    start = time.time()
    fb_dataframe_map_numeric_column(fb_df, "int_col", lambda x: x * 2)
    fb_dataframe_map_numeric_column(fb_df, "float_col", lambda x: x / 2)
    fb_map_time = time.time() - start

    df_dill_bytestring = dill.dumps(df)

    # Time deserializing with dill, performing the operations, then re-serializing.
    start = time.time()
    df_deserialized = dill.loads(df_dill_bytestring)
    df_deserialized["int_col"] = df_deserialized["int_col"].apply(lambda x: x * 2)
    df_deserialized["float_col"] = df_deserialized["float_col"].apply(lambda x: x / 2)
    df_dill_bytestring = dill.dumps(df_deserialized)
    dill_map_time = time.time() - start

    # Modifying in place should be faster.
    assert fb_map_time < dill_map_time, "Your implementation should be faster than deserializing the entire dataframe with dill, performing the operation, then re-serializing."
