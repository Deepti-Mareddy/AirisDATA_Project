# Campaign_Project

## Table : Campaign_dataset

There are 15 columns in Campaign dataset

```
Search_Item : String
Match_Type : String          
Include_Exclude : String          
Campaign_Name : String          
Group : String          
Displayed : Int         
Clicks : Int          
Interaction_Rate : Double          
Currency_Code : String          
Per_Click_Rate : Double          
Amount_Paid : Double          
Conversion : Int          
No_Conversion : Int          
%Conversion : Double          
Identity : Int
```

## Problem Statement

An online IT course training provider Company running an Add Campaign has their own Campaign data set.The Campaign name is Future Search India.<br>
Company provides online training for various topics. Company has to provide the charges to Local Search engine that provides local search related services to users across India. <br>
The charges that Company has to pay, based on per click by user. Total cost depends upon Amount_Paid. 

NOTE: Search_Item consists of many Keywords. For eg. In "big data engineer" Search_Item consists of 3 keywords.<br>
   For Eg:     Search_Item ->			Amount_Paid<br>
   				--------------------<br>
 				Big data engineer -> 500<br>
				Analytic engineer -> 200<br>
        
Amount Paid for Big, data, Analytic is 500 each<br>
Amount Paid for engineer is 500+200=700<br>

So company needs to know for which keyword they have to pay the maximum amount to Local Search engine provider. Create a Spark scala program for that.
