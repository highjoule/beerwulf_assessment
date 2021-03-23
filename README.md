# Python and mysql

This code shows how to process information of large data sets using SQL and Python.

In the outcome one will be able to see top and bottom countries in terms of sales, most common use of ship mode from top country, top client in terms of revenue, and a review in terms of revenue from fiscal years from 01 july to 30 of june.


```python

```

# Connection to mysql


```python
import mysql.connector
from mysql.connector import Error
import pandas as pd
```

## Function that will connect the mysql server


```python
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            allow_local_infile=True
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection
```

## Connection to server using my credentials


```python
pw='julio'#please change the password to the users password
connection = create_server_connection("localhost", "root", pw)
```

    MySQL Database connection successful
    


```python
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")
```

## Creation of database called MYDB


```python
create_database_query = 'CREATE DATABASE MYDB'
execute_query(connection,create_database_query)
```

    Query succesful
    


```python
create_database_query = 'USE MYDB'
execute_query(connection,create_database_query)
```

    Query successful
    

## Before continuing with the task, I will add revenue to the lineitem table using the formula:

Revenue = Extended price - discount - tax; since I assumed that the extended price comes as a calculation made from this columns.


```python
import pandas as pd
```

### File is read, and I add revenue and an ID columns to this table.


```python
l_df0 = pd.read_table('lineitem.tbl',delimiter='|',header=None,names=['order','part','suppkey','linenumber','quantity','extendedprice','discount','tax','returnflag','linestatus','shipdate','commit','receipt','instruct','shipmode','comment','revenue','ID'])
l_df0
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order</th>
      <th>part</th>
      <th>suppkey</th>
      <th>linenumber</th>
      <th>quantity</th>
      <th>extendedprice</th>
      <th>discount</th>
      <th>tax</th>
      <th>returnflag</th>
      <th>linestatus</th>
      <th>shipdate</th>
      <th>commit</th>
      <th>receipt</th>
      <th>instruct</th>
      <th>shipmode</th>
      <th>comment</th>
      <th>revenue</th>
      <th>ID</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1552</td>
      <td>93</td>
      <td>1</td>
      <td>17</td>
      <td>24710.35</td>
      <td>0.04</td>
      <td>0.02</td>
      <td>N</td>
      <td>O</td>
      <td>1996-03-13</td>
      <td>1996-02-12</td>
      <td>1996-03-22</td>
      <td>DELIVER IN PERSON</td>
      <td>TRUCK</td>
      <td>egular courts above the</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>674</td>
      <td>75</td>
      <td>2</td>
      <td>36</td>
      <td>56688.12</td>
      <td>0.09</td>
      <td>0.06</td>
      <td>N</td>
      <td>O</td>
      <td>1996-04-12</td>
      <td>1996-02-28</td>
      <td>1996-04-20</td>
      <td>TAKE BACK RETURN</td>
      <td>MAIL</td>
      <td>ly final dependencies: slyly bold</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>637</td>
      <td>38</td>
      <td>3</td>
      <td>8</td>
      <td>12301.04</td>
      <td>0.10</td>
      <td>0.02</td>
      <td>N</td>
      <td>O</td>
      <td>1996-01-29</td>
      <td>1996-03-05</td>
      <td>1996-01-31</td>
      <td>TAKE BACK RETURN</td>
      <td>REG AIR</td>
      <td>riously. regular, express dep</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>22</td>
      <td>48</td>
      <td>4</td>
      <td>28</td>
      <td>25816.56</td>
      <td>0.09</td>
      <td>0.06</td>
      <td>N</td>
      <td>O</td>
      <td>1996-04-21</td>
      <td>1996-03-30</td>
      <td>1996-05-16</td>
      <td>NONE</td>
      <td>AIR</td>
      <td>lites. fluffily even de</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>241</td>
      <td>23</td>
      <td>5</td>
      <td>24</td>
      <td>27389.76</td>
      <td>0.10</td>
      <td>0.04</td>
      <td>N</td>
      <td>O</td>
      <td>1996-03-30</td>
      <td>1996-03-14</td>
      <td>1996-04-01</td>
      <td>NONE</td>
      <td>FOB</td>
      <td>pending foxes. slyly re</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>60170</th>
      <td>60000</td>
      <td>1843</td>
      <td>44</td>
      <td>2</td>
      <td>23</td>
      <td>40131.32</td>
      <td>0.05</td>
      <td>0.03</td>
      <td>N</td>
      <td>O</td>
      <td>1995-08-09</td>
      <td>1995-06-08</td>
      <td>1995-08-23</td>
      <td>COLLECT COD</td>
      <td>FOB</td>
      <td>ideas about the permanent</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>60171</th>
      <td>60000</td>
      <td>1057</td>
      <td>63</td>
      <td>3</td>
      <td>45</td>
      <td>43112.25</td>
      <td>0.02</td>
      <td>0.02</td>
      <td>R</td>
      <td>F</td>
      <td>1995-05-15</td>
      <td>1995-05-31</td>
      <td>1995-06-03</td>
      <td>COLLECT COD</td>
      <td>SHIP</td>
      <td>fully bold pinto beans alongside</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>60172</th>
      <td>60000</td>
      <td>271</td>
      <td>53</td>
      <td>4</td>
      <td>29</td>
      <td>33966.83</td>
      <td>0.02</td>
      <td>0.01</td>
      <td>N</td>
      <td>O</td>
      <td>1995-07-25</td>
      <td>1995-06-07</td>
      <td>1995-08-17</td>
      <td>COLLECT COD</td>
      <td>SHIP</td>
      <td>ly final ideas boost s</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>60173</th>
      <td>60000</td>
      <td>585</td>
      <td>16</td>
      <td>5</td>
      <td>31</td>
      <td>46052.98</td>
      <td>0.00</td>
      <td>0.05</td>
      <td>N</td>
      <td>O</td>
      <td>1995-08-06</td>
      <td>1995-07-18</td>
      <td>1995-08-19</td>
      <td>TAKE BACK RETURN</td>
      <td>TRUCK</td>
      <td>ly even instr</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>60174</th>
      <td>60000</td>
      <td>836</td>
      <td>3</td>
      <td>6</td>
      <td>45</td>
      <td>78157.35</td>
      <td>0.04</td>
      <td>0.08</td>
      <td>N</td>
      <td>O</td>
      <td>1995-07-23</td>
      <td>1995-07-17</td>
      <td>1995-07-24</td>
      <td>DELIVER IN PERSON</td>
      <td>TRUCK</td>
      <td>ke final packages. carefully final fo</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>60175 rows × 18 columns</p>
</div>



### Calculation of revenue


```python
l_df0['revenue']=l_df0['extendedprice']+l_df0['discount']+l_df0['tax']
```

### A unique idex just in case.


```python
l_df0['ID'] = [i+1 for i in range(l_df0.index.size)]
```

### Loading of the table as a table file, using new name and keep the original.


```python
l_df0.to_csv('lineitem2.tbl', header=None, index=None, sep='|', line_terminator='|\n')
```

### Read the table again just to make sure that the infromation was correctly saved. The columns migth not match with the intended data, but luckily for SQL this is irrelevant at this moment.


```python
l_df = pd.read_table('lineitem2.tbl',delimiter='|',header=None,names=['order','part','suppkey','linenumber','quantity','extendedprice','discount','tax','returnflag','linestatus','shipdate','commit','receipt','instruct','shipmode','comment','revenue'])
l_df
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>order</th>
      <th>part</th>
      <th>suppkey</th>
      <th>linenumber</th>
      <th>quantity</th>
      <th>extendedprice</th>
      <th>discount</th>
      <th>tax</th>
      <th>returnflag</th>
      <th>linestatus</th>
      <th>shipdate</th>
      <th>commit</th>
      <th>receipt</th>
      <th>instruct</th>
      <th>shipmode</th>
      <th>comment</th>
      <th>revenue</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="5" valign="top">1</th>
      <th>1552</th>
      <td>93</td>
      <td>1</td>
      <td>17</td>
      <td>24710.35</td>
      <td>0.04</td>
      <td>0.02</td>
      <td>N</td>
      <td>O</td>
      <td>1996-03-13</td>
      <td>1996-02-12</td>
      <td>1996-03-22</td>
      <td>DELIVER IN PERSON</td>
      <td>TRUCK</td>
      <td>egular courts above the</td>
      <td>24710.41</td>
      <td>1</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>674</th>
      <td>75</td>
      <td>2</td>
      <td>36</td>
      <td>56688.12</td>
      <td>0.09</td>
      <td>0.06</td>
      <td>N</td>
      <td>O</td>
      <td>1996-04-12</td>
      <td>1996-02-28</td>
      <td>1996-04-20</td>
      <td>TAKE BACK RETURN</td>
      <td>MAIL</td>
      <td>ly final dependencies: slyly bold</td>
      <td>56688.27</td>
      <td>2</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>637</th>
      <td>38</td>
      <td>3</td>
      <td>8</td>
      <td>12301.04</td>
      <td>0.10</td>
      <td>0.02</td>
      <td>N</td>
      <td>O</td>
      <td>1996-01-29</td>
      <td>1996-03-05</td>
      <td>1996-01-31</td>
      <td>TAKE BACK RETURN</td>
      <td>REG AIR</td>
      <td>riously. regular, express dep</td>
      <td>12301.16</td>
      <td>3</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>22</th>
      <td>48</td>
      <td>4</td>
      <td>28</td>
      <td>25816.56</td>
      <td>0.09</td>
      <td>0.06</td>
      <td>N</td>
      <td>O</td>
      <td>1996-04-21</td>
      <td>1996-03-30</td>
      <td>1996-05-16</td>
      <td>NONE</td>
      <td>AIR</td>
      <td>lites. fluffily even de</td>
      <td>25816.71</td>
      <td>4</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>241</th>
      <td>23</td>
      <td>5</td>
      <td>24</td>
      <td>27389.76</td>
      <td>0.10</td>
      <td>0.04</td>
      <td>N</td>
      <td>O</td>
      <td>1996-03-30</td>
      <td>1996-03-14</td>
      <td>1996-04-01</td>
      <td>NONE</td>
      <td>FOB</td>
      <td>pending foxes. slyly re</td>
      <td>27389.90</td>
      <td>5</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>...</th>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th rowspan="5" valign="top">60000</th>
      <th>1843</th>
      <td>44</td>
      <td>2</td>
      <td>23</td>
      <td>40131.32</td>
      <td>0.05</td>
      <td>0.03</td>
      <td>N</td>
      <td>O</td>
      <td>1995-08-09</td>
      <td>1995-06-08</td>
      <td>1995-08-23</td>
      <td>COLLECT COD</td>
      <td>FOB</td>
      <td>ideas about the permanent</td>
      <td>40131.40</td>
      <td>60171</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1057</th>
      <td>63</td>
      <td>3</td>
      <td>45</td>
      <td>43112.25</td>
      <td>0.02</td>
      <td>0.02</td>
      <td>R</td>
      <td>F</td>
      <td>1995-05-15</td>
      <td>1995-05-31</td>
      <td>1995-06-03</td>
      <td>COLLECT COD</td>
      <td>SHIP</td>
      <td>fully bold pinto beans alongside</td>
      <td>43112.29</td>
      <td>60172</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>271</th>
      <td>53</td>
      <td>4</td>
      <td>29</td>
      <td>33966.83</td>
      <td>0.02</td>
      <td>0.01</td>
      <td>N</td>
      <td>O</td>
      <td>1995-07-25</td>
      <td>1995-06-07</td>
      <td>1995-08-17</td>
      <td>COLLECT COD</td>
      <td>SHIP</td>
      <td>ly final ideas boost s</td>
      <td>33966.86</td>
      <td>60173</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>585</th>
      <td>16</td>
      <td>5</td>
      <td>31</td>
      <td>46052.98</td>
      <td>0.00</td>
      <td>0.05</td>
      <td>N</td>
      <td>O</td>
      <td>1995-08-06</td>
      <td>1995-07-18</td>
      <td>1995-08-19</td>
      <td>TAKE BACK RETURN</td>
      <td>TRUCK</td>
      <td>ly even instr</td>
      <td>46053.03</td>
      <td>60174</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>836</th>
      <td>3</td>
      <td>6</td>
      <td>45</td>
      <td>78157.35</td>
      <td>0.04</td>
      <td>0.08</td>
      <td>N</td>
      <td>O</td>
      <td>1995-07-23</td>
      <td>1995-07-17</td>
      <td>1995-07-24</td>
      <td>DELIVER IN PERSON</td>
      <td>TRUCK</td>
      <td>ke final packages. carefully final fo</td>
      <td>78157.47</td>
      <td>60175</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>60175 rows × 17 columns</p>
</div>



# Task classify client's account balance

## Read table file and add valuable headers to the dataframe


```python
c_df0 = pd.read_table('customer.tbl',delimiter='|',header=None,names=['custindex','custkey','name','c','d','balance','f','g','class'])
```

## Classification balance

This function sorts the balance of the clients and classifies them in 3 groups:
- Top 20: Of all the clients with a positive balance, these are the 20% who has the largest amount.
- Not top 20%, but some debt: These are the rest of the mentioned above, they still have a debt to the company but not as high as the group above.
- Negative balance: These selected group have a negative balance in their accounts with the company.


```python
def debt_class(c_df):
    
    c_df=c_df.sort_values(by=['balance'],ascending=False)#reads the table of customers
    clas = ['top 20% debt','current debt','positive balance','not a number in balance']#categories
    
    
    dim = range(0,c_df.index.size)#size where the for will make a loop
    neg_range = []#range of the positive numbers in the balance
    
    for i in dim:
        try:#this is in case a non float is read, the prgram tells the user there is something suspicious here
            val = c_df['balance'].iloc[i]#value to evaluate
            if val<0:#if negative the code labels these client in the column 'class'
                c_df['class'].iloc[i]=clas[2]
            else:#if positive the code labels these
                neg_range.append(i)
                c_df['class'].iloc[i]=clas[1]
        except ValueError:
            c_df['class'].iloc[i]=clas[3]

    top20=round(len(neg_range)*0.2,0)#size of the ones with positive balance
    top20_range = range(0,int(top20))#size of the top20%
    
    for i in top20_range:c_df['class'].iloc[i]=clas[0]#classification of the top 20% classes
    
    return(c_df.sort_index())#return back the modified dataframe using the original indexing
```

## Validation that the operations where made


```python
c_df=debt_class(c_df0)
c_df
```


    




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>custindex</th>
      <th>custkey</th>
      <th>name</th>
      <th>c</th>
      <th>d</th>
      <th>balance</th>
      <th>f</th>
      <th>g</th>
      <th>class</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Customer#000000001</td>
      <td>IVhzIApeRb ot,c,E</td>
      <td>15</td>
      <td>25-989-741-2988</td>
      <td>711.56</td>
      <td>BUILDING</td>
      <td>to the even, regular platelets. regular, ironi...</td>
      <td>current debt</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Customer#000000002</td>
      <td>XSTf4,NCwDVaWNe6tEgvwfmRchLXak</td>
      <td>13</td>
      <td>23-768-687-3665</td>
      <td>121.65</td>
      <td>AUTOMOBILE</td>
      <td>l accounts. blithely ironic theodolites integr...</td>
      <td>current debt</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Customer#000000003</td>
      <td>MG9kdTD2WBHm</td>
      <td>1</td>
      <td>11-719-748-3364</td>
      <td>7498.12</td>
      <td>AUTOMOBILE</td>
      <td>deposits eat slyly ironic, even instructions....</td>
      <td>current debt</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Customer#000000004</td>
      <td>XxVSJsLAGtn</td>
      <td>4</td>
      <td>14-128-190-5944</td>
      <td>2866.83</td>
      <td>MACHINERY</td>
      <td>requests. final, regular ideas sleep final accou</td>
      <td>current debt</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Customer#000000005</td>
      <td>KvpyuHCplrB84WgAiGV6sYpZq7Tj</td>
      <td>3</td>
      <td>13-750-942-6364</td>
      <td>794.47</td>
      <td>HOUSEHOLD</td>
      <td>n accounts will have to unwind. foxes cajole a...</td>
      <td>current debt</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1495</th>
      <td>1496</td>
      <td>Customer#000001496</td>
      <td>ZOyMxutVHpJy</td>
      <td>3</td>
      <td>13-802-978-9538</td>
      <td>-496.49</td>
      <td>AUTOMOBILE</td>
      <td>counts wake slyly above the instructi</td>
      <td>positive balance</td>
    </tr>
    <tr>
      <th>1496</th>
      <td>1497</td>
      <td>Customer#000001497</td>
      <td>D8e2U3gYd57H4grcOr,02</td>
      <td>14</td>
      <td>24-506-574-8552</td>
      <td>2449.57</td>
      <td>AUTOMOBILE</td>
      <td>gular packages boost foxes. blithely bold esca...</td>
      <td>current debt</td>
    </tr>
    <tr>
      <th>1497</th>
      <td>1498</td>
      <td>Customer#000001498</td>
      <td>x XToT5oFi7oIsRG2mgIL3ncvYJoWBsufsQ7N,z</td>
      <td>19</td>
      <td>29-676-227-6356</td>
      <td>5810.56</td>
      <td>AUTOMOBILE</td>
      <td>ackages are slyly unusual req</td>
      <td>current debt</td>
    </tr>
    <tr>
      <th>1498</th>
      <td>1499</td>
      <td>Customer#000001499</td>
      <td>4,6jWOEqfnuXkwhB7gs0M9TcWJlaJNv4bt</td>
      <td>3</td>
      <td>13-273-527-9609</td>
      <td>9128.69</td>
      <td>AUTOMOBILE</td>
      <td>ole blithely permanent instructions. carefully...</td>
      <td>top 20% debt</td>
    </tr>
    <tr>
      <th>1499</th>
      <td>1500</td>
      <td>Customer#000001500</td>
      <td>4zaoUzuWUTNFiNPbmu43</td>
      <td>5</td>
      <td>15-200-872-4790</td>
      <td>6910.79</td>
      <td>MACHINERY</td>
      <td>s boost blithely above the fluffily ironic dol...</td>
      <td>current debt</td>
    </tr>
  </tbody>
</table>
<p>1500 rows × 9 columns</p>
</div>



## Add a bar chart to see the information


```python
clas = ['top 20% debt','current debt','positive balance']

bar1=sum(c_df['balance'][c_df['class']==clas[0]])
bar2=sum(c_df['balance'][c_df['class']==clas[1]])
bar3=sum(c_df['balance'][c_df['class']==clas[2]])

bars = [round(bar1,2),round(bar2,2),round(bar3,2)]

pos = [1,3,5]
```


```python
import matplotlib.pyplot as plt


plt.bar(pos,bars, label=clas)
plt.ylabel('Monetary unit')
plt.title('Debt amount')
plt.xticks(pos,clas)
plt.grid(b=True, which='major', axis = 'y' , color='#A79E9E', linestyle='-', linewidth=0.4)
plt.minorticks_on()
plt.grid(b=True, which='minor', axis = 'y' , color='#999999', linestyle='-', alpha=0.2)
plt.show()
print(bars)
```


![](https://github.com/highjoule/pythonsql/blob/main/class_customer.png)


    [2438817.05, 4314693.49, -71644.95]
    

## Store of data back in the location of files to the next task


```python
c_df.to_csv('customer2.tbl', header=None, index=None, sep='|', line_terminator='|\n')
```

## Quick verification that the data saved is the same as the one modified here


```python
c_df_ver = pd.read_table('customer2.tbl',delimiter='|',header=None,names=['custkey','name','b','c','d','balance','f','g','class'])
c_df_ver
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>custkey</th>
      <th>name</th>
      <th>b</th>
      <th>c</th>
      <th>d</th>
      <th>balance</th>
      <th>f</th>
      <th>g</th>
      <th>class</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1</th>
      <td>Customer#000000001</td>
      <td>IVhzIApeRb ot,c,E</td>
      <td>15</td>
      <td>25-989-741-2988</td>
      <td>711.56</td>
      <td>BUILDING</td>
      <td>to the even, regular platelets. regular, ironi...</td>
      <td>current debt</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Customer#000000002</td>
      <td>XSTf4,NCwDVaWNe6tEgvwfmRchLXak</td>
      <td>13</td>
      <td>23-768-687-3665</td>
      <td>121.65</td>
      <td>AUTOMOBILE</td>
      <td>l accounts. blithely ironic theodolites integr...</td>
      <td>current debt</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Customer#000000003</td>
      <td>MG9kdTD2WBHm</td>
      <td>1</td>
      <td>11-719-748-3364</td>
      <td>7498.12</td>
      <td>AUTOMOBILE</td>
      <td>deposits eat slyly ironic, even instructions....</td>
      <td>current debt</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Customer#000000004</td>
      <td>XxVSJsLAGtn</td>
      <td>4</td>
      <td>14-128-190-5944</td>
      <td>2866.83</td>
      <td>MACHINERY</td>
      <td>requests. final, regular ideas sleep final accou</td>
      <td>current debt</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Customer#000000005</td>
      <td>KvpyuHCplrB84WgAiGV6sYpZq7Tj</td>
      <td>3</td>
      <td>13-750-942-6364</td>
      <td>794.47</td>
      <td>HOUSEHOLD</td>
      <td>n accounts will have to unwind. foxes cajole a...</td>
      <td>current debt</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1496</th>
      <td>Customer#000001496</td>
      <td>ZOyMxutVHpJy</td>
      <td>3</td>
      <td>13-802-978-9538</td>
      <td>-496.49</td>
      <td>AUTOMOBILE</td>
      <td>counts wake slyly above the instructi</td>
      <td>positive balance</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1497</th>
      <td>Customer#000001497</td>
      <td>D8e2U3gYd57H4grcOr,02</td>
      <td>14</td>
      <td>24-506-574-8552</td>
      <td>2449.57</td>
      <td>AUTOMOBILE</td>
      <td>gular packages boost foxes. blithely bold esca...</td>
      <td>current debt</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1498</th>
      <td>Customer#000001498</td>
      <td>x XToT5oFi7oIsRG2mgIL3ncvYJoWBsufsQ7N,z</td>
      <td>19</td>
      <td>29-676-227-6356</td>
      <td>5810.56</td>
      <td>AUTOMOBILE</td>
      <td>ackages are slyly unusual req</td>
      <td>current debt</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1499</th>
      <td>Customer#000001499</td>
      <td>4,6jWOEqfnuXkwhB7gs0M9TcWJlaJNv4bt</td>
      <td>3</td>
      <td>13-273-527-9609</td>
      <td>9128.69</td>
      <td>AUTOMOBILE</td>
      <td>ole blithely permanent instructions. carefully...</td>
      <td>top 20% debt</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1500</th>
      <td>Customer#000001500</td>
      <td>4zaoUzuWUTNFiNPbmu43</td>
      <td>5</td>
      <td>15-200-872-4790</td>
      <td>6910.79</td>
      <td>MACHINERY</td>
      <td>s boost blithely above the fluffily ironic dol...</td>
      <td>current debt</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>1500 rows × 9 columns</p>
</div>



# Back to the mysql, I build the needed database and load information in the tables


```python
use_query = 'USE MYDB'
execute_query(connection,create_database_query)
```

    Query successful
    

## Under the connection with mysql server, the tables are created as follow

### Create Region


```python
create_table="""CREATE TABLE REGION (
  R_REGIONKEY INTEGER PRIMARY KEY NOT NULL,
  R_NAME      TEXT NOT NULL,
  R_COMMENT   TEXT
);
"""
execute_query(connection,create_table)
```

    Query successful
    

### Create Part


```python
create_table="""CREATE TABLE PART (
  P_PARTKEY     INTEGER PRIMARY KEY NOT NULL,
  P_NAME        TEXT NOT NULL,
  P_MFGR        TEXT NOT NULL,
  P_BRAND       TEXT NOT NULL,
  P_TYPE        TEXT NOT NULL,
  P_SIZE        INTEGER NOT NULL,
  P_CONTAINER   TEXT NOT NULL,
  P_RETAILPRICE INTEGER NOT NULL,
  P_COMMENT     TEXT NOT NULL
);"""
execute_query(connection,create_table)
```

    Query successful
    

### Create Nation


```python
create_table="""CREATE TABLE NATION (
  N_NATIONKEY INTEGER PRIMARY KEY NOT NULL,
  N_NAME      TEXT NOT NULL,
  N_REGIONKEY INTEGER NOT NULL,
  N_COMMENT   TEXT,
  FOREIGN KEY (N_REGIONKEY) REFERENCES REGION(R_REGIONKEY)
);"""
execute_query(connection,create_table)
```

    Query successful
    

### Create Supplier


```python
create_table="""CREATE TABLE SUPPLIER (
  S_SUPPKEY   INTEGER PRIMARY KEY NOT NULL,
  S_NAME      TEXT NOT NULL,
  S_ADDRESS   TEXT NOT NULL,
  S_NATIONKEY INTEGER NOT NULL,
  S_PHONE     TEXT NOT NULL,
  S_ACCTBAL   INTEGER NOT NULL,
  S_COMMENT   TEXT NOT NULL,
  FOREIGN KEY (S_NATIONKEY) REFERENCES NATION(N_NATIONKEY)
);
"""
execute_query(connection,create_table)

```

    Query successful
    

### Create PartSupp


```python
create_table="""CREATE TABLE PARTSUPP (
  PS_PARTKEY    INTEGER NOT NULL,
  PS_SUPPKEY    INTEGER NOT NULL,
  PS_AVAILQTY   INTEGER NOT NULL,
  PS_SUPPLYCOST INTEGER NOT NULL,
  PS_COMMENT    TEXT NOT NULL,
  PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY),
  FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER(S_SUPPKEY),
  FOREIGN KEY (PS_PARTKEY) REFERENCES PART(P_PARTKEY)
);"""
execute_query(connection,create_table)
```

    Query successful
    

### Create Customer


```python
create_table="""
CREATE TABLE CUSTOMER (
  C_CUSTKEY    INTEGER PRIMARY KEY NOT NULL,
  C_NAME       TEXT NOT NULL,
  C_ADDRESS    TEXT NOT NULL,
  C_NATIONKEY  INTEGER NOT NULL,
  C_PHONE      TEXT NOT NULL,
  C_ACCTBAL    INTEGER   NOT NULL,
  C_MKTSEGMENT TEXT NOT NULL,
  C_COMMENT    TEXT NOT NULL,
  C_CLASSJ     TEXT NOT NULL,
  FOREIGN KEY (C_NATIONKEY) REFERENCES NATION(N_NATIONKEY)
);"""
execute_query(connection,create_table)
```

    Query successful
    

### Create Orders


```python
create_table="""
CREATE TABLE ORDERS (
  O_ORDERKEY      INTEGER PRIMARY KEY NOT NULL,
  O_CUSTKEY       INTEGER NOT NULL,
  O_ORDERSTATUS   TEXT NOT NULL,
  O_TOTALPRICE    INTEGER NOT NULL,
  O_ORDERDATE     DATE NOT NULL,
  O_ORDERPRIORITY TEXT NOT NULL,  
  O_CLERK         TEXT NOT NULL, 
  O_SHIPPRIORITY  INTEGER NOT NULL,
  O_COMMENT       TEXT NOT NULL,
  FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER(C_CUSTKEY)
);"""
execute_query(connection,create_table)
```

    Query successful
    

### Create Lineitem


```python
create_table="""
CREATE TABLE LINEITEM (
  L_ORDERKEY      INTEGER NOT NULL,
  L_PARTKEY       INTEGER NOT NULL,
  L_SUPPKEY       INTEGER NOT NULL,
  L_LINENUMBER    INTEGER NOT NULL,
  L_QUANTITY      INTEGER NOT NULL,
  L_EXTENDEDPRICE INTEGER NOT NULL,
  L_DISCOUNT      INTEGER NOT NULL,
  L_TAX           INTEGER NOT NULL,
  L_RETURNFLAG    TEXT NOT NULL,
  L_LINESTATUS    TEXT NOT NULL,
  L_SHIPDATE      DATE NOT NULL,
  L_COMMITDATE    DATE NOT NULL,
  L_RECEIPTDATE   DATE NOT NULL,
  L_SHIPINSTRUCT  TEXT NOT NULL,
  L_SHIPMODE      TEXT NOT NULL,
  L_COMMENT       TEXT NOT NULL,
  L_REVENUEITEM   INTEGER NOT NULL,
  L_ID            INTEGER NOT NULL,
  PRIMARY KEY (L_ORDERKEY, L_LINENUMBER,L_ID),
  FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS(O_ORDERKEY),
  FOREIGN KEY (L_PARTKEY, L_SUPPKEY) REFERENCES PARTSUPP(PS_PARTKEY, PS_SUPPKEY)
);"""
execute_query(connection,create_table)
```

    Query successful
    

## Load information from tables

In this part I am loading the tables to the database created, particularly for the case of Lineitem and Partsupp tables, one has to execute this code line, I could not see the reason but I assume that this is due to the size of Lineitem.

So please, if the reader is trying to execute this, I encourage to reload Lineitem and Partsupp cell code couple times. 

### Use DataBase


```python
use_db="USE MYDB"
execute_query(connection,use_db)
```

    Query successful
    

### Load Region


```python
load_table="""
LOAD DATA LOCAL INFILE 'region.tbl' 
INTO TABLE region
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';
"""
execute_query(connection,load_table)
```

    Query successful
    

### Load Nation


```python
load_table="""
LOAD DATA LOCAL INFILE 'nation.tbl' 
INTO TABLE nation
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';
"""
execute_query(connection,load_table)
```

    Query successful
    

### Load Supplier


```python
load_table="""
LOAD DATA LOCAL INFILE 'supplier.tbl' 
INTO TABLE supplier
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';
"""
execute_query(connection,load_table)
```

    Query successful
    

### Load Customer


```python
load_table="""
LOAD DATA LOCAL INFILE 'customer2.tbl' 
INTO TABLE customer
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';
"""
execute_query(connection,load_table)
```

    Query successful
    

### Load Orders


```python
load_table="""
LOAD DATA LOCAL INFILE 'orders.tbl' 
INTO TABLE orders
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';
"""
execute_query(connection,load_table)
```

    Query successful
    

### Load PartSupp


```python
load_table="""
LOAD DATA LOCAL INFILE 'partsupp.tbl' 
INTO TABLE partsupp
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';
"""
execute_query(connection,load_table)
```

    Query successful
    

### Load Lineitem


```python
load_table="""
LOAD DATA LOCAL INFILE 'lineitem2.tbl' 
INTO TABLE lineitem
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';
"""
execute_query(connection,load_table)
```

    Query successful
    

### Load Part


```python
load_table="""
LOAD DATA LOCAL INFILE 'part.tbl' 
INTO TABLE part
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';
"""
execute_query(connection,load_table)
```

    Query successful
    

## Once all the data is loaded, I will try to retrieve all the neccesary data that may allow me to retrieve important information.

### Connection to server and the database called MYDB


```python
pw='julio'
connection = create_server_connection("localhost", "root", pw)
use_query = 'USE MYDB'
execute_query(connection,use_query)
```

    MySQL Database connection successful
    Query successful
    

### These function will allow me to execute SQL codes and retrieve information from the databases


```python
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")
```

### Top costumer in terms of both quantity and revenue


```python
q1 = """
SELECT LINEITEM.L_ORDERKEY, LINEITEM.L_LINENUMBER, LINEITEM.L_REVENUEITEM,LINEITEM.L_QUANTITY, ORDERS.O_CUSTKEY, CUSTOMER.C_NAME
FROM LINEITEM
INNER JOIN ORDERS ON ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY
INNER JOIN CUSTOMER ON ORDERS.O_CUSTKEY = CUSTOMER.C_CUSTKEY
ORDER BY LINEITEM.L_REVENUEITEM ASC;"""

db = 'MYDB'

def top_customer(q1,pw,db,concept):

    connection = create_db_connection("localhost", "root", pw, db)#connection to database
    results = read_query(connection, q1)#results stored in this variable

    from_db = []#empty list to store one line of results

    for result in results:
      result = list(result)
      from_db.append(result)#storage of all the lines from the empty list


    columns = ["order", "linenumber", "revenue","quantity","customerkey", "customername"]#columns in the dataframe
    df = pd.DataFrame(from_db, columns=columns)#dataframe of the list with the results from the SQL's query

    columns1 = ["customer",concept]#concept to analyze
    aux = []#auxiliar variable
    
    for i in df['customername'].unique():#total of revenue/quantity of everyclient is stored in the next datframe
        j = df.loc[df['customername'] == i, concept].sum()
        aux.append([i,j])

    dfaux=pd.DataFrame(aux,columns=columns1)#this dataframe has every client, with its sum of revenue/quantity 
    
    return dfaux[dfaux[concept]==dfaux[concept].max()]#returns the client with max revenue/quantity
```

### Top customer in terms of revenue


```python
concept = 'revenue'
top_customer(q1,pw,db,concept)
```

    MySQL Database connection successful
    




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer</th>
      <th>revenue</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>633</th>
      <td>Customer#000001489</td>
      <td>5457282</td>
    </tr>
  </tbody>
</table>
</div>



### Top customer in terms of quantity


```python
concept = 'quantity'
top_customer(q1,pw,db,concept)
```

    MySQL Database connection successful
    




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer</th>
      <th>quantity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>633</th>
      <td>Customer#000001489</td>
      <td>3868</td>
    </tr>
  </tbody>
</table>
</div>



## Top and bottom nations in terms of revenue


```python
q2 = """
SELECT LINEITEM.L_ORDERKEY, LINEITEM.L_LINENUMBER, LINEITEM.L_REVENUEITEM, LINEITEM.L_SHIPMODE, ORDERS.O_CUSTKEY, CUSTOMER.C_NATIONKEY, NATION.N_NAME
FROM LINEITEM
INNER JOIN ORDERS ON ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY
INNER JOIN CUSTOMER ON ORDERS.O_CUSTKEY = CUSTOMER.C_CUSTKEY
INNER JOIN NATION ON CUSTOMER.C_NATIONKEY = NATION.N_NATIONKEY
ORDER BY LINEITEM.L_REVENUEITEM ASC;"""

db = 'mydb'



def top_bottom(pw,db,q2):

    connection = create_db_connection("localhost", "root", pw, db)#connection to database
    results = read_query(connection, q2)#results stored in this variable

    from_db = []#empty list to store one line of results

    for result in results:
      result = list(result)
      from_db.append(result)#storage of all the lines from the empty list


    columns = ["order", "linenumber", "revenue","shipmode","customerkey", "nationkey","nationname"]#columns in the dataframe
    df2 = pd.DataFrame(from_db, columns=columns)#dataframe of the list with the results

    columns1 = ['nationname','revenue']#concept to analyze
    aux = []#auxiliar variable

    for i in df2['nationname'].unique():#total of revenue/quantity of everyclient is stored in the next datframe
        j = df2.loc[df2['nationname'] == i, 'revenue'].sum()
        aux.append([i,j])

    dfaux1=pd.DataFrame(aux,columns=columns1)#this dataframe has every client, with its sum of revenue/quantity 

    bottom_nations = dfaux1.sort_values(by='revenue')[0:3]#bottom 3 nations sorted by revenue
    top_nations = dfaux1.sort_values(by='revenue',ascending=False)[0:3]#top 3 nations sorted by revenue
    
    return df2,bottom_nations,top_nations
```

### Bottom, top and dataframe from the database


```python
df2,bottom_nations,top_nations = top_bottom(pw,db,q2)
```

    MySQL Database connection successful
    

### Names of top and bottom nations


```python
bottom_names=bottom_nations['nationname']
top_names=top_nations['nationname']
```

### Bottom nations


```python
bottom_nations
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>nationname</th>
      <th>revenue</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>24</th>
      <td>FRANCE</td>
      <td>54431802</td>
    </tr>
    <tr>
      <th>7</th>
      <td>CHINA</td>
      <td>65868743</td>
    </tr>
    <tr>
      <th>21</th>
      <td>UNITED STATES</td>
      <td>65951430</td>
    </tr>
  </tbody>
</table>
</div>



### Top Nations 


```python
top_nations
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>nationname</th>
      <th>revenue</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CANADA</td>
      <td>110895322</td>
    </tr>
    <tr>
      <th>11</th>
      <td>EGYPT</td>
      <td>107539560</td>
    </tr>
    <tr>
      <th>9</th>
      <td>IRAN</td>
      <td>105586147</td>
    </tr>
  </tbody>
</table>
</div>



### Top ship mode


```python
def top_shipmode(df2,country):

    c_list = []
    for c in country:
        value = df2.loc[df2['nationname'] == c, 'shipmode'].value_counts().max()#max value of the shipmode from country
        shipmode = df2.loc[df2['nationname'] == c, 'shipmode'].value_counts().idxmax()#description of shipmode with max value
        revenue = df2.loc[df2['nationname'] == c, 'revenue'].sum()#records revenue of country
        c_list.append([c,shipmode,value,revenue])#value, shipmode and revenue are atored in this list

    columns2=['top country','top shipmode','ship value','revenue']#headers of dataframe
    df3 = pd.DataFrame(c_list,columns =columns2)#makes a dataframe
    return df3
```

### Top ship mode from top countries


```python
top_custmode=top_shipmode(df2,top_names)
top_custmode
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>top country</th>
      <th>top shipmode</th>
      <th>ship value</th>
      <th>revenue</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CANADA</td>
      <td>FOB</td>
      <td>502</td>
      <td>110895322</td>
    </tr>
    <tr>
      <th>1</th>
      <td>EGYPT</td>
      <td>REG AIR</td>
      <td>446</td>
      <td>107539560</td>
    </tr>
    <tr>
      <th>2</th>
      <td>IRAN</td>
      <td>MAIL</td>
      <td>428</td>
      <td>105586147</td>
    </tr>
  </tbody>
</table>
</div>



## Calculation of top months and foscal year revenue


```python
q3 = """
SELECT LINEITEM.L_ORDERKEY, LINEITEM.L_LINENUMBER, LINEITEM.L_REVENUEITEM, ORDERS.O_ORDERDATE
FROM LINEITEM
INNER JOIN ORDERS ON ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY
ORDER BY LINEITEM.L_REVENUEITEM ASC;"""

db = 'MYDB'

def months(pw,db,q3):

    connection = create_db_connection("localhost", "root", pw, db)#connection to database
    results = read_query(connection, q3)#results stored in this variable

    from_db = []#empty list to store one line of results

    for result in results:
      result = list(result)
      from_db.append(result)#storage of all the lines from the empty list


    columns = ["order", "linenumber", "revenue","date"]#columns in the dataframe
    df3 = pd.DataFrame(from_db, columns=columns)#dataframe of the list with the results

    date_series = pd.to_datetime(df3['date'])#change index in datetime format
    date_index = pd.DatetimeIndex(date_series.values)
    date_series = df3.set_index(date_index)#set the date as index

    monthly_series = date_series.resample('M').sum()#resample by month
    monthly_series['year']=monthly_series.index.year#get the year
    monthly_series['month']=monthly_series.index.month#get the month
    monthly_series['day']=monthly_series.index.day#get the day

    top_months=monthly_series.sort_values(by='revenue',ascending=False)[0:10]#top 10 months
    
    return monthly_series,top_months
```

### Monthly series and top months in terms of revenue



```python
monthly_series,top_months = months(pw,db,q3)
```

    MySQL Database connection successful
    

### Top 10 months


```python
top_months[['revenue','year','month']]
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>revenue</th>
      <th>year</th>
      <th>month</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1993-10-31</th>
      <td>31176250</td>
      <td>1993</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1993-12-31</th>
      <td>31122581</td>
      <td>1993</td>
      <td>12</td>
    </tr>
    <tr>
      <th>1992-01-31</th>
      <td>30878824</td>
      <td>1992</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1996-08-31</th>
      <td>30497311</td>
      <td>1996</td>
      <td>8</td>
    </tr>
    <tr>
      <th>1995-12-31</th>
      <td>30439242</td>
      <td>1995</td>
      <td>12</td>
    </tr>
    <tr>
      <th>1994-01-31</th>
      <td>30322813</td>
      <td>1994</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1993-09-30</th>
      <td>30234304</td>
      <td>1993</td>
      <td>9</td>
    </tr>
    <tr>
      <th>1994-05-31</th>
      <td>30015138</td>
      <td>1994</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1994-09-30</th>
      <td>29820218</td>
      <td>1994</td>
      <td>9</td>
    </tr>
    <tr>
      <th>1992-03-31</th>
      <td>29758338</td>
      <td>1992</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



## Grouping of months in fiscal year in therms of revenue


```python
def make_fiscal_months_list(starts,monthly_series):# this function gives the cycle of the fiscal year, if starts in july(7), ends in june(6)

    monthly_series['month'].unique()#months
    st_over = 1#restart of cylce
    fiscal_list = []#empty list to store the cycle
    for k in monthly_series['month'].unique():#cycle from 1 to 12
        if k+starts < 14:#stores until the count gets to 12
            fiscal_list.append(k+starts-1)#stores first months until 12
        else:#once 12 is reached the count restarts and fills the list from 1 onwards
            fiscal_list.append(st_over)
            st_over += 1#counter from the next months
    return fiscal_list

```

### This function makes posible get fiscal year grouping


```python
def make_fiscal_year_list(monthly_series):    
    years_fiscal=list(monthly_series['year'].unique()-1)
    last=years_fiscal[-1]
    years_fiscal.append(last+1)
    return years_fiscal
```

### This function calculates the fiscal year starting from 01 july to june 30


```python

starts = 7#starting month of fiscal year 1 for january, 2 for february, ... , 12 for december, this case 
fiscal_months = make_fiscal_months_list(starts,monthly_series)
fiscal_years = make_fiscal_year_list(monthly_series)

def fiscal_year_series(starts,monthly_series,fiscal_months,fiscal_years):
    aux_month = []

    try:
        for i in fiscal_years:
            revenue_acc = 0
            for j in fiscal_months:
                try:
                    if j>(starts-1):
                        revenue_acc += monthly_series[str(i)].loc[monthly_series['month']==j]['revenue'][0]
                    else:
                        revenue_acc += monthly_series[str(i+1)].loc[monthly_series['month']==j]['revenue'][0]
                except :
                    if i == fiscal_years[-1]:
                        break
                    else:
                        pass
            aux_month.append([i,revenue_acc])
    except IndexError:
        pass
    
    return aux_month

```

### Convert aux months lists to DataFrame


```python
aux_month=fiscal_year_series(starts,monthly_series,fiscal_months,fiscal_years)
```

### To check the information is complete, I sum up all the revenue


```python
columns=['year','revenue']
df_year_fiscal = pd.DataFrame(aux_month,columns=columns)
df_year_fiscal['revenue'].sum()
```




    2152195594



### I sum up the monthly series, to verify the complete operation from the original database


```python
monthly_series['revenue'].sum()
```




    2152195594



### Check that both numbers are the same


```python
df_year_fiscal['revenue'].sum() - monthly_series['revenue'].sum()
```




    0



### Resume of fiscal year revenue 


```python
df_year_fiscal
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>year</th>
      <th>revenue</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1991</td>
      <td>166760467</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1992</td>
      <td>316313183</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1993</td>
      <td>342781195</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1994</td>
      <td>317891748</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1995</td>
      <td>325880837</td>
    </tr>
    <tr>
      <th>5</th>
      <td>1996</td>
      <td>335095004</td>
    </tr>
    <tr>
      <th>6</th>
      <td>1997</td>
      <td>318227754</td>
    </tr>
    <tr>
      <th>7</th>
      <td>1998</td>
      <td>29245406</td>
    </tr>
  </tbody>
</table>
</div>



### Graph of the information from the table above


```python
ax = df_year_fiscal.plot.bar(x='year')
```


![](https://github.com/highjoule/pythonsql/blob/main/fiscal_year.png)


# Star schema

The schema below, I show the fact table applying for this case regarding SQL data management.


```python
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
img = mpimg.imread('starschema.png')
imgplot = plt.imshow(img)
plt.show()
```


![](https://github.com/highjoule/pythonsql/blob/main/star_schema.png)




```python

```


```python

```


```python

```


```python

```
