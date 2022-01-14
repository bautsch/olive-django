# olive-django

## OLIVE: Optimizing Linux-based Integrated Volumetrics and Economics

### What problem are you trying to solve?

Generating economics on a single well with monthly resolution is fast.
Generating economics on a single well with daily resolution is slow.
Generating economics on thousands of wells with daily resolution takes forever.
Running a monte carlo simulation with unique inputs and distributions and generating economics on thousands of wells with daily resolution takes longer than the DMV.

12 months for 50 years 	=	600 time steps

365.25 days for 50 years	=	18,262.5 time steps

**About 1,500X more data for one well!**

Olive dramatically improves not just computation time but also write time.


### What does Olive do?

Consists of a user frontend run through a web browser, a Python codebase to compute data, and a SQL database backend to store output

Neat features include:
- Multiprocessing! Can run across as many cores as Jardine will give me
- Spiffy user interface with buttons (new feature in version 2!)
- Can apply normal, uniform, exponential, and binomial distributions
- Can autofit type curves within a specified range
- Can flex IP90 without impacting EUR
- Very efficient data connection to SQL Server
- Sophisticated natural language processing for economics
- Auto calculates percentiles for all output
- Overengineered paper trail, nothing is deleted
- Convenient use of CSVs to export and import data

![screenshot of olive frontend](https://github.com/bautsch/olive-django/blob/master/images/example.png?raw=true)
