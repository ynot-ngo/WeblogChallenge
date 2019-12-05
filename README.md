# PayTM Weblog Challenge

## Overview:

This assignment was developed in an Ubuntu VM in Jupyter Notebooks using Python version 3.6.9 and Spark version 2.4.4.
The solution was implemented using a variety of Pandas DataFrames, Spark DataFrames, registering Spark DataFrames as tables (in the absence of a DBMS on the VM) and SparkSQL.

Please find the log analyzer.ipynb in the root folder which has comments inline for clarity of the code. 

## Additional Notes:

IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions.

There are additional methods for identifying visitors, sessions and paths:

IP-Agent, in addition to IP address you can use user-agent (browser) information to determine unique sessions. This will help detect multiple users from one IP address.

Username data if the website is configured to require authentication.

Session ID, if found in URL query or stored in a Cookie can result in more accurate visitor session and path analysis.

In addition, we can also filter entries which do not add value to the analytics. Requests that are made by automatic activity such as bots can be filtered. Or some requests for components of the webpage such as to images may not be relevant.


