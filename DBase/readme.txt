This is a very simple way to run the code for linux(ubuntu)

1.	Install java; the exact version I used is 1.8
2.	Install sbt : https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html (for linux --- i used ububtu)
Or run the following lines in your terminal
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list

sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823

sudo apt-get update

sudo apt-get install sbt

3.	I have attached a file (build.sbt ) to this email, put it inside DBase folder ,the main folder along side src ,target ,project, etc.
4.	I suppose you have spark installed already.
5.	Open terminal and navigate to DBASE.
6.	Enter:  sbt compile
7.	Enter:  sbt run
8.	You should see this in the terminal 
Multiple main classes detected, select one to run:
[1] program1
      [2] program2

9.	Press 1 or 2 for program 1 or 2 and press enter.
10.	If you don’t set the hdfs paths before running, there will be a runtime error.
11.	To make it simple, I already included some of the files to the zip I sent you earlier;
a.	For program 1 you can set the path to "src/main/resources/dataset" this folder is included in what I sent you and it should be able to read the input files.
b.	On line 168, you can set the path for where your model should be saved.  But that should be the same as the one in program 2 line 117.


I.	For program 2, you can read inputs files from "src/main/resources/dataset" folder, set path on line 41. (you can always set it to your hdfs directory)
II.	On line 117 you specify the path to the saved model(Same as b above)
III.	On line 191 you can specify the path to your saved transactions.

*Setting the paths to the files is very important

I can also come around to set it up for you also.


*****Any changes you make, you should use sbt.compile to compile and then run it with sbt.run

Thank you!
