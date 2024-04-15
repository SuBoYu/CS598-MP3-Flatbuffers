# Project 3
CS 598 YP Spring 2024\
**Last Updated:** March 25th 2024\
**Deadline:** April 18th 2024, 11:59 PM CT

## Project Overview
In this project, you will be working with Google's [Flatbuffers](https://github.com/google/flatbuffers) and Python's [shared memory](https://docs.python.org/3/library/multiprocessing.shared_memory.html) libraries to pass serialized Dataframes between notebook sessions, and perform various operations (`head`, `groupby`, `map`) directly on the serialized Dataframes.

## Getting Started
As with the previous projects, you will need to [clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repository to your own Github account - please do not commit directly to this repository!

This repository contains submodules. Therefore, you will need to run `git clone` with the `--recurse-submodules` option:
```
git clone --recurse-submodules https://github.com/illinoisdata/CS598-MP3-Flatbuffers
```
You will then need to make your cloned repository **private**.

**Note**: if you forget to use the `--recurse-submodules` flag when cloning, you can still initialize the Flatbuffer submodule with a separate command as follows:
```
git clone https://github.com/illinoisdata/CS598-MP3-Flatbuffers   # Oops, I forgot to --recurse-submodules
cd CS598-MP3-Flatbuffers
git submodule update --init --recursive
```

### Building the Flatbuffers Binary

The next step is build the Flatbuffers binary (included as a submodule in this repository under `flatbuffers`):

```
cd flatbuffers

# Depending on your OS, run one of these:
cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release        # Run this if you are on Linux or MacOS
cmake -G "Visual Studio 10" -DCMAKE_BUILD_TYPE=Release
cmake -G "Xcode" -DCMAKE_BUILD_TYPE=Release

make -j
```

### Flatbuffer Definition Files

Flatbuffers definition files (`.fbs` suffix, see `monster.fbs` for an example) are similar to JSON schema files; they specify the fields that will be present in the serialized data. `fbs` files need to be compiled into Python modules first via your newly built Flatbuffers binary before they can be used:
```
# Go back to mp3-flatbuffers directory
cd ..

# Compile the example flatbuffer defition file
flatbuffers/flatc --python monster.fbs
```

You can find the resulting Python modules under `MyGame/Sample`; the path of the generated module is determined by the namespace declaration (`namespace MyGame.Sample;`) at the top of `monster.fbs`. The [Flatbffers tutorial page](https://flatbuffers.dev/flatbuffers_guide_tutorial.html) provides an in-depth explanation of how to write to and access the fields; select 'Python' for the Python-specific API.

<img width="421" alt="image" src="https://github.com/illinoisdata/CS598-MP3-Flatbuffers/assets/31910858/cf6bb95f-326c-4587-a5d6-72d10fd2aba2">

## Project Motivation

A motivating use case for Flatbuffers is illustrated in the `host_nb.ipynb` and `guest_nb.ipynb` notebooks. Suppose you want to pass a dataframe (`climatechange_tweets_all.csv`) from `host_nb.ipynb` to `guest_nb.ipynb`. A standard method would to be use Python's [shared memory](https://docs.python.org/3/library/multiprocessing.shared_memory.html): the shared memory is accessible from any Python process on the same machine, with the limitation that it can only hold raw bytes, that is, the dataframe will need to be serialized before it can be placed in shared memory. 

Therefore, to pass the dataframe, the `host_nb` will serialize the dataframe with `Pickle`, Python's de-facto serialization protocol, put the serialized bytes into shared memory, then `guest_nb` will deserialize the bytes back (again using `Pickle`) into a dataframe to perform the desired operations (e.g., `head`). Below is an illustration of this process: To experience this process yourself, run the first 3 cells in `host_nb` followed by the first 3 cells in `guest_nb`.

![Screenshot 2024-03-25 at 3 00 45 PM](https://github.com/illinoisdata/CS598-MP1-OLA/assets/31910858/09beb749-4bf9-4f3f-8c94-dfbae3914619)

The problem with message passing via serialization/deserialization with `Pickle` is that in `guest_nb`, the dataframe must be entirely deserialized first before any operations can be performed, even if the operation only accesses a small part of the dataframe (e.g., `head` accessing first 5 rows). This is significant time and space overhead - the user will have to wait for the deserialization to complete, while their namespace will also contain a copy of the dataframe from `host_nb`.

This is where Flatbuffers comes in. A main selling point of Flatbuffers is that **individual fields can be accessed without deserializing the Flatbuffer**; that is, if the dataframe was serialized as a Flatbuffer in `host_nb`, the head of the dataframe can be computed in `guest_nb` by only reading the relevant rows - no deserialization of the entire dataframe needed:

![image](https://github.com/illinoisdata/CS598-MP1-OLA/assets/31910858/5c56a4fb-55a5-4424-9c13-7790f3a34322)

## Tasks

Your tasks are as follows:
- Writing the Flatbuffers definition file for Dataframes and code for serializing Dataframes into Flatbuffers (30%)
- Performing the `head()` operation directly on a Flatbuffer-serialized dataframe (10%)
- Performing the `sum(x) group by y` operation directly on a Flatbuffer-serialized dataframe (10%)
- Modifying a column in Flatbuffer-serialized dataframe in-place via `map` (30%)
- Integrating your Flatbuffer functions with shared memory (20%)

You will be filling out the Flatbuffer definition in `dataframe.fbs`, Flatbuffers operations in `fb_dataframe.py`, and shared memory-related operations in `fb_shared_memory.py`. The [Flatbffers tutorial page](https://flatbuffers.dev/flatbuffers_guide_tutorial.html) and the [shared memory](https://docs.python.org/3/library/multiprocessing.shared_memory.html) pages contain many helpful examples for completing your tasks. The detailed requirements for these tasks are given below. **It is important that you understand the operations performed in the Flatbuffers tutorial - you will need to use them for the tasks in the project.**

## Serializing Dataframes into Flatbuffers (30%)

Your Flatbuffer definition for Dataframes should support the `int64`, `float`, and `string` (called `object` in Pandas Dataframes) datatype columns. The definition should follow a *columnar layout* as illustrated below:
```
+-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
| DF metadata | col 1 metadata | val 1 | val 2 | ... | col 2 metadata | val 1 | val 2 | ... |
+-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
```
Where `metadata` can contain anything you see fit (e.g., column names, data types), and `val1, val2` are actual values in the Dataframe columns. 

Once written and compiled with `flatbuffers/flatc --python dataframe.fbs`, you will write the `to_flatbuffer` function in `fb_dataframe.py` to serialize a Pandas Dataframe into a bytearray according to your Flatbuffer definition:
- `int` and `float` columns should be stored as `int` and `float64` values in the flatbuffer using `AddIntdata` and `AddFloatdata` respectively - **don't convert them to strings!** This is both less efficient and loses precision for floats.
- `object` columns should be stored as `string` values using `CreateString`.

## Performing the HEAD Operation on Flatbuffer-Serialized Dataframes (10%)
You will write the `fb_dataframe_head` function in `fb_dataframe.py` to directly read the first `n` rows from a Flatbuffer-serialized Dataframe. The results should be returned as a Pandas Dataframe.

You will be graded on the efficiency of the implementation: performing `head` on a Flatbuffer-serialized Dataframe should be faster than deserializing with `Pickle`, then calling `head` (see `test_fb_dataframe_head` in `test_fb_dataframe.py` for the test case).

## Performing Grouped Aggregation on Flatbuffer-serialized Dataframes (10%)
You will write the `fb_dataframe_group_by_sum` function in `fb_dataframe.py` to compute grouped aggregates for `sum_col` grouping by `group_col` on a Flatbuffer-serialized Dataframe. The results should be returned as a Pandas Dataframe.

You will be graded on the efficiency of the implementation: performing `head` on a Flatbuffer-serialized Dataframe should be faster than deserializing with `Pickle`, then calling `agg` (see `test_fb_dataframe_group_by_sum` in `test_fb_dataframe.py` for the test case).

## Modifying Flatbuffer-serialized Dataframes In-place via map (30%)
You will write the `fb_dataframe_map_numeric_column` function in `fb_dataframe.py` to modify a column (`map_col`) in the Flatbuffer-serialized Dataframe in-place by applying the `map` function (e.g., `lambda x: x * 2`) to all elements in `map_col`. Your implementation only needs to support cases where `map_col` is an `int` or `float` column - it should not do anything if `map_col` is a `string` column.

**Hints on getting started:** 

You will find this helpful message in the Flatbuffers tutorial page regarding mutating Flatbuffers in Python:

`<API for mutating FlatBuffers is not yet available in Python.>`

This means you will have to do this task without a dedicated API via some old-fashioned byte manipulation (it's worth 30% for a reason). Note that whenever you create a Flatbuffer object (e.g., `builder.CreateString`), its offset relative to the end of the Flatbuffer is returned. If you know the offset (i.e., where the object is located in the bytestring), you can directly manipulate it without using the Flatbuffer API. Running a brute-force scan may be helpful to know how items are serialized in the Flatbuffer:
```
for i in range(a, b):
  try:
    print(a, int.from_bytes(buf[a:a+8], 'little'))
  except:
    pass
```

You will be graded on the efficiency of the implementation: performing `map` on a Flatbuffer-serialized Dataframe should be faster than deserializing with `Pickle`, performing the `map`, then re-serializing the mapped Dataframe (see `test_fb_dataframe_map_numeric_column` in `test_fb_dataframe.py` for the test case).

## Integrating your Flatbuffer functions with shared memory (20%)

You are almost done! The last step is to integrate the above Flatbuffer functions with Python's shared memory. You will need to complete 2 class functions of `FbSharedMemory` located in `fb_shared_memory.py`. 
- `add_dataframe`: Adds a dataframe with the name `name` to the shared memory.
- `_get_fb_buf`: Returns the shared memory buffer section corresponding to the Flatbuffer-serialized dataframe `name`.

Recall that to read an object stored in shared memory from another process, you will need to know the offset of where the object is stored in the buffer - your implementation should correctly map Dataframe `names` to the offsets containing the Flatbuffer-serialized Dataframes. Your implementation should support access to multiple stored Dataframes at once.

**Hints on getting started:**

In addition to the Dataframes in shared memory, you will also need to share the offset mappings between notebook processes. There are many options for this such as writing and reading the mappings to and from a file or using a second shared memory.

## Grading

You will be graded on both the correctness and efficiency of your implementations. You can check your assignment progress via the Github Actions workflow. If the actions workflow passes, you will receive full score for this assignment. 

You should not modify `test_fb_dataframe.py` or `test_fb_shared_memory.py` as they are used by the autograder.

## Submission instructions

You will submit your work for Project 1 by uploading the URL of your private repository to the Project 3 - Flatbuffers assignment to Canvas. You will also need to share access to your private repository to the two course TAs:
- Billy Li (BillyZhaohengLi)
- Hanxi Fang (iq180fq200)

**You will also need to upload your generated Flatbuffers directory (e.g., if your namespace for `dataframe.fbs` is `CS598.MP3`, upload the `CS598` directory) to your repository for the autograder to run.**

## For Fun

Once you have received full credit for the assignment, you can run `host_nb.py` followed by `guest_nb.py` to see benchmarking results of unpickling the Dataframe and performing the operation versus performing the operation directly with your Flatbuffers implementations.

## FAQ
- Q1: Why are my column and row orders reversed?
- A1: Note that adding to Flatbuffer vectors can only be done with `Prepend` and not `Append` - you will need to adjust the insert order accordingly.
- Q2: Why am I getting `object serialization cannot be nested`?
- A2: As the message suggests, you cannot be working on multiple Flatbuffer objects at the same time. A notable example is when creating a vector of strings:
```
# This doesn't work - the strings were created after creating the vector.
Fb.StartStringVector(builder, len(offsets))
for item in str_list:
    builder.PrependSOffsetTRelative(builder.CreateString(str(item)))
data = builder.EndVector()

# This works - the strings were created before creating the vector.
offsets = [builder.CreateString(str(item)) for item in str_list]

Fb.StartStringVector(builder, len(offsets))
for i in offsets:
    builder.PrependSOffsetTRelative(i)
data = builder.EndVector()
```
- Q3: Why is my integer decoding function returning weird numbers for the `map` question?
- A4: There are 2 ways to encode integers - little and big endian. Try to figure out which one Flatbuffer uses.
- Q4: Is it expected that in `guest_nb`, performing Groupby with my Flatbuffer implementation is slower than unserializing with Dill, then performing Groupby?
- A4: That is very possible. In general, serializing and deserializing Dataframes with Flatbuffers is much slower than doing so with Pickle. That is why the benefits of Flatbuffers only show if the amount of data you are reading between using Flatbuffers and Pickle are vastly different (e.g., 5 rows vs. 450,000 rows for `head`, respectively). The 2 out of 15 columns you are reading with Flatbuffers is already a sizable enough portion of the Dataframe to make it slower than Unpickling.
