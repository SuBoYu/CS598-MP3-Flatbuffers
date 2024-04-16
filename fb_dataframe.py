import flatbuffers
import pandas as pd
import struct
import time
import types
from Dataframe import DataFrame, Column, ColMetaData, DataType, IntData, FloatData, StringData

# Your Flatbuffer imports here (i.e. the files generated from running ./flatc with your Flatbuffer definition)...

def to_flatbuffer(df: pd.DataFrame) -> bytes:
    """
        Converts a DataFrame to a flatbuffer. Returns the bytes of the flatbuffer.

        The flatbuffer should follow a columnar format as follows:
        +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
        | DF metadata | col 1 metadata | val 1 | val 2 | ... | col 2 metadata | val 1 | val 2 | ... |
        +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
        You are free to put any bookkeeping items in the metadata. however, for autograding purposes:
        1. Make sure that the values in the columns are laid out in the flatbuffer as specified above
        2. Serialize int and float values using flatbuffer's 'PrependInt64' and 'PrependFloat64'
            functions, respectively (i.e., don't convert them to strings yourself - you will lose
            precision for floats).

        @param df: the dataframe.
    """
    builder = flatbuffers.Builder(1024)
    int_col_list = list()
    float_col_list = list()
    str_col_list = list()

    for c_name in reversed(df.columns):
        if df[c_name].dtype == "int64":
            datatype = DataType.DataType().INT64
            IntData.IntDataStartDataVector(builder, len(df[c_name]))
            for v in reversed(df[c_name]):
                builder.PrependInt64(v)
            datas = builder.EndVector()
            IntData.Start(builder)
            IntData.AddData(builder, datas)
            c_data = IntData.End(builder)

            c_name = builder.CreateString(c_name)

            ColMetaData.Start(builder)
            ColMetaData.AddName(builder, c_name)
            ColMetaData.AddType(builder, datatype)
            c_metadata = ColMetaData.End(builder)

            Column.Start(builder)
            Column.AddColmetadata(builder, c_metadata)
            Column.AddData(builder, c_data)
            int_col_list.append(Column.End(builder))
        elif df[c_name].dtype == "float64":
            datatype = DataType.DataType().FLOAT64
            FloatData.FloatDataStartDataVector(builder, len(df[c_name]))
            for v in reversed(df[c_name]):
                builder.PrependFloat64(v)
            datas = builder.EndVector()
            FloatData.Start(builder)
            FloatData.AddData(builder, datas)
            c_data = FloatData.End(builder)

            c_name = builder.CreateString(c_name)

            ColMetaData.Start(builder)
            ColMetaData.AddName(builder, c_name)
            ColMetaData.AddType(builder, datatype)
            c_metadata = ColMetaData.End(builder)

            Column.Start(builder)
            Column.AddColmetadata(builder, c_metadata)
            Column.AddData(builder, c_data)
            float_col_list.append(Column.End(builder))
        else:
            datatype = DataType.DataType().STRING
            str_offsets = list()
            for v in reversed(df[c_name]):
                str_offsets.append(builder.CreateString(v))
            StringData.StringDataStartDataVector(builder, len(str_offsets))
            for offset in str_offsets:
                builder.PrependUOffsetTRelative(offset)
            datas = builder.EndVector()
            StringData.Start(builder)
            StringData.AddData(builder, datas)
            c_data = StringData.End(builder)

            c_name = builder.CreateString(c_name)

            ColMetaData.Start(builder)
            ColMetaData.AddName(builder, c_name)
            ColMetaData.AddType(builder, datatype)
            c_metadata = ColMetaData.End(builder)

            Column.Start(builder)
            Column.AddColmetadata(builder, c_metadata)
            Column.AddData(builder, c_data)
            str_col_list.append(Column.End(builder))

    col_list = int_col_list + float_col_list + str_col_list

    DataFrame.DataFrameStartColumnsVector(builder, len(col_list))
    for col in col_list:
        # print(col)
        builder.PrependUOffsetTRelative(col)
    columns = builder.EndVector(len(col_list))

    DataFrame.DataFrameStart(builder)
    DataFrame.AddColumns(builder, columns)
    dataframe = DataFrame.DataFrameEnd(builder)

    builder.Finish(dataframe)


    return builder.Output()  # REPLACE THIS WITH YOUR CODE...

def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    """
        Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
        similar to df.head(). If there are less than n rows, return the entire Dataframe.
        Hint: don't forget the column names!

        @param fb_bytes: bytes of the Flatbuffer Dataframe.
        @param rows: number of rows to return.
    """
    return pd.DataFrame()  # REPLACE THIS WITH YOUR CODE...


def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    """
        Applies GROUP BY SUM operation on the flatbuffer dataframe grouping by grouping_col_name
        and summing sum_col_name. Returns the aggregate result as a Pandas dataframe.

        @param fb_bytes: bytes of the Flatbuffer Dataframe.
        @param grouping_col_name: column to group by.
        @param sum_col_name: column to sum.
    """
    return pd.DataFrame()  # REPLACE THIS WITH YOUR CODE...


def fb_dataframe_map_numeric_column(fb_buf: memoryview, col_name: str, map_func: types.FunctionType) -> None:
    """
        Apply map_func to elements in a numeric column in the Flatbuffer Dataframe in place.
        This function shouldn't do anything if col_name doesn't exist or the specified
        column is a string column.

        @param fb_buf: buffer containing bytes of the Flatbuffer Dataframe.
        @param col_name: name of the numeric column to apply map_func to.
        @param map_func: function to apply to elements in the numeric column.
    """
    # YOUR CODE HERE...
    pass
    