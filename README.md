# Lambda Architecture Hadoop

A simple java springboot application to generate a Tag Cloud in three ways.
1. Generating a Tagcloud using non-big-data tools for quickly generating and showing a result to the user.
2. Generating a Tagcloud from the uploaded file by using a hadoop job.
3. Generating a Tagcloud from the corpus of all uploaded files using a hadoop job.

## How to run
In eclipse rightclick on LambdaApp.java and select "Run Java Application". You can now access localhost:8080 on your browser and upload text documents in the minimal UI. You will see the batch jobs triggered asynchronously and files getting updated, as soon as the batch jobs are finished. This simple example demonstrates how to process even large quantities of data might be possible on a scalable architecture.

## Credits
I did this work in my Big-Data course at the university of applied sciences. The template (springboot app) for this task was given by Prof. Dr. Hummel. You can access the template here: https://git.informatik.hs-mannheim.de/o.hummel/Hadoop
