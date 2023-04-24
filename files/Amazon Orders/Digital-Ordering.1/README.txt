This is a guide on how to view your digital ordering data.
Some data fields will have the data value "N/A". This means that there was no data present for that field.
It is recommended to view the CSV files in below order.

Digital Orders.csv
- This file has an overview of your digital orders such as eBooks, MP3s and subscriptions purchased through Amazon.

Digital Items.csv
- This csv has data related to each Item that was part of a Digital Order.
- Use the OrderId field in this csv to connect data between the Digital Orders.csv and this csv.

Digital Orders Monetary.csv
- This file has data related to the monetary information about each item purchased such as the amount paid for an item and the amount paid for tax (where applicable).
- Use the DigitalOrderItemId field to connect data between the Digital Items.csv and this file.
- Use the DeliveryPacketId field to connect data between to the Digital Order.csv and this file.

Legacy Payment Information.csv
- This file contains additional payment information that is not captured in the Digital Orders.csv such as the payment details and type of payment that was used for an order.
- Use the OrderId field to connect data between the Digital Orders.csv and this file.