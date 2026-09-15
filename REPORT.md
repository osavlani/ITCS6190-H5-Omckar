# Hands-on L5: Report

**Name:Omckar Savlani**
**Student ID: 801497440**
**Email: osavlani@charlotte.edu**

---

## What I ran

` ` `

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

git diff de49a3f c34406f -- wordcount.py > /tmp/wc.diff
code /tmp/wc.diff
:To get code changes in wordcount.py file, transferred changes to a file as terminal (of codespace) keeps truncating output.

` ` `

---

## Input and output


### My input dataset (song lyrics)

```

Purple hat, cheetah print
Dancing on the people, rolled up at the after joint
Dancing dancing on the people
People dancing on the people, I got people on the people
People dancing on the people
With the people on the people
Smoke and CO2
See me see you, dancing on the people
Climb up on the booth, hanging from the people
On the people
My head hits the roof, dancing on the ceiling on the people
I got people on the people
Dancing dancing on the people
I've got purple hat, cheetah print
Dancing on the people
Rolled up at the after joint
Dancing dancing on the people
People dancing on the people
I got people on the people
People dancing on the people
With the people on the people
Sofi Tukker

```

### The output of part 1 

Paste the contents of the `part-...txt` file from `shared-folder/output/wordcount/`.

```
the 25
people 21
dancing 9
Dancing 5
People 4
got 4
With 2
after 2
cheetah 2
hat, 2
joint 2
people, 2
print 2
CO2 1
Climb 1
I've 1
Purple 1
Rolled 1
See 1
Smoke 1
Sofi 1
Tukker 1
and 1
booth, 1
ceiling 1
from 1
hanging 1
head 1
hits 1
purple 1
rolled 1
roof, 1
see 1
you, 1
```

---

## What I observed

A few sentences on what you actually noticed. Some things worth looking at:

- What the master page at <http://localhost:8080> showed when the shell connected
- How many tasks and executors the Spark UI at <http://localhost:4040> listed for `show`
- How long the job took, in the shell and with `spark-submit`

I didn't note time taken by job but it was done in under or around ~12s. Setting up the image via docker took upto 120s.

Even after changing the function to normalize alphabet cases, if processed with punctuation for eg "the" and "the," both are considered seperate values as they compare as different strings. This happened because we set the space/empty character as a defining function to identify different strings. We can change the function to be more accurate by adding more conditions while defining the function.

---

## What I changed

` ` `

from pyspark.sql import SparkSession
-from pyspark.sql.functions import explode, split, length, col
+from pyspark.sql.functions import explode, split, length, col, lower
 
 if len(sys.argv) < 2:
     print(__doc__)
     sys.exit(2)
 
+min_len = int(sys.argv[3]) if len(sys.argv) > 3 else 5
+
 spark = SparkSession.builder.appName("WordCount").getOrCreate()
 
 lines = spark.read.text(sys.argv[1])
 words = lines.select(explode(split(col("value"), r"\s+")).alias("word"))
-counts = (words.filter(length("word") >= 3)
+counts = (words.withColumn("word", lower(col("word")))
+               .filter(length("word") >= min_len)
                .groupBy("word").count()
                .orderBy(col("count").desc(), col("word")))
 
+normalized = words.withColumn("word", lower(col("word")))
+total_words = normalized.count()
+kept_words  = normalized.filter(length("word") >= min_len).count()
+
+print(f"{total_words} words scanned")
+print(f"{kept_words} words of at least {min_len} characters")
+

```python

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

The original function had 34 unique strings, after modification to normalize alphabet cases the unique strings normalized to 29 (eg 'People', 'Dancing'); aka 5 matching words with different cases were found. After truncating with min_len >=5 it reduced to 16 as less words are considered overall.


### Jobs

How many jobs did your run launch, according to the **Jobs** tab, and how does that compare
with the original program? Why does Spark read the same file more than once in a single run?



---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.

For step 9 part 2(min_len >=5) no folder was created as I copied previous command, and v2 dir already existed. Fixed that by identifying the correction
step 9 dosent mention to copy wordcount.py again after min_len >=5 change. I'm not sure if change updates automatically but I copied file to working dir again before executing the job. (not mentioned to copy again in steps)



