# Hands-on L5: Report

**Name:Omckar Savlani**
**Student ID: 801497440**
**Email: osavlani@charlotte.edu**

---

## What I ran

The commands you used, in the order you used them. If you deviated from the steps in the
README, say where and why.

docker --version
:To confirm docker configuration

docker compose -f docker-compose.codespaces.yml up -d
:To compose docker with codespace file, path defined in the command

docker exec -it spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077
:To open the PySpark shell

docker ps
:To check if the nodes are live and running

from pyspark.sql.functions import explode, split, length, col
lines = spark.read.text("/opt/spark/work-dir/shared/input/data/input.txt")
words = lines.select(explode(split(col("value"), r"\s+")).alias("word"))
counts = words.filter(length("word") >= 3).groupBy("word").count()
counts.orderBy(col("count").desc(), col("word")).show()
:running the wordcount function directly from terminal

counts.orderBy("word").show()
:Table output is ordered by characters in the word instead of their overall count

docker cp wordcount.py spark-master:/opt/spark/work-dir/
:Copying wordcount.py from main to the working dir inside the shell

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/wordcount.py \
  /opt/spark/work-dir/shared/input/data/input.txt \
  /opt/spark/work-dir/shared/output/wordcount
:Assigning the master node our wordcount.py file for execution. Input and output dir and filenames also specified.

docker cp wordcount.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/wordcount.py \
  /opt/spark/work-dir/shared/input/data/input.txt \
  /opt/spark/work-dir/shared/output/wordcount-v2
:Copying changed file to working directory, and output file is given a different name

docker cp wordcount.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/wordcount.py \
  /opt/spark/work-dir/shared/input/data/input.txt \
  /opt/spark/work-dir/shared/output/wordcount-long 5
:Copying file again after further changes, again with a different name specified for output going to be generated

docker compose -f docker-compose.codespaces.yml down
:To compose down the docker containers, codespace file forced in the command
---

## Input and output


### My input dataset

```

```

### The output of part 1

Paste the contents of the `part-...txt` file from `shared-folder/output/wordcount/`.

```

```

---

## What I observed

A few sentences on what you actually noticed. Some things worth looking at:

- What the master page at <http://localhost:8080> showed when the shell connected
- How many tasks and executors the Spark UI at <http://localhost:4040> listed for `show`
- How long the job took, in the shell and with `spark-submit`



---

## What I changed

The three changes you made to `wordcount.py`. Paste the lines you added or rewrote
(`git diff` gives you exactly this).

```python

```

---

## What the changes did

### The three numbers

| Run | Min length | Words scanned | Words kept | Distinct words |
| --- | ---------- | ------------- | ---------- | -------------- |
| `wordcount-v2` | 3 | | | |
| `wordcount-long` | | | | |

### The three outputs compared

How many distinct words did folding the case remove (compare `wordcount/` with
`wordcount-v2/`)? How many did the longer minimum remove? Name one word from your own text
whose count changed when the counting became case-insensitive.



### Jobs

How many jobs did your run launch, according to the **Jobs** tab, and how does that compare
with the original program? Why does Spark read the same file more than once in a single run?



---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.


