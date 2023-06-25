#Here Four -Conflicting transactions are:
#1-A customer adds a product to their cart and proceeds to checkout.
#2-A customer applies a discount code to their order and the discount is successfully applied.
#3-A customer enters their shipping and billing information and completes the purchase.
#4-A customer receives an order confirmation email with the details of their purchase.

#Here Four Non-Conflicting transactions are:
#1-A customer tries to apply a discount code to their order , but the code is invalid or expired.
#2-A customer enters their shipping and billing information but their payment is declined.

import json
from flask import Flask, render_template, request, redirect, session
import mysql.connector
import ast,datetime
import time
app = Flask(__name__)
app.secret_key = 'apnabazzzar'

# MySQL configuration
# MySQL configurations
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="apnabazzzardb"
)

@app.route('/')
def home():
    return render_template('root.html')

@app.route('/adminpage')
def adminpage():
    return render_template('admin.html')
@app.route('/admin', methods=['POST'])
def admin():
    username = request.form['username']
    password = request.form['password']

    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM admin_table WHERE username = %s AND password = %s", (username, password))
    myresult = mycursor.fetchall()

    if len(myresult) == 0:
        return "You are not registered"
    else:
        return render_template("adminhome.html",username=username)
##########################################################################################
#Olap query using group by
@app.route('/listordersName', methods=['POST','GET'])
def listordersName():
    mycursor = mydb.cursor()
    mycursor.execute("select U.Name ,count(*) as number_of_order from order_table o ,user_table U where U.id=o.id and o.id IN (select id from order_table where TotalCost>=0) group by U.Name")
    data = mycursor.fetchall()
    return render_template("listofordersName.html", data=data)

@app.route('/listdeliverdetails', methods=['POST','GET'])
def listdeliverdetails():
    return render_template("nameofshippername.html")

@app.route('/searchedname',methods=['POST','GET'])
def listddetails():
    name = request.form['name']
    l=[]
    l.append(name)
    mycursor = mydb.cursor()
    mycursor.execute("select user_table.Name,order_table.Order_Id,order_table.Order_street,order_table.Order_city,order_table.Order_pincode,shipper_table.shipper_name from ((order_table inner join shipper_table  on order_table.shipper_id=shipper_table.shipper_id )inner join user_table on order_table.id=user_table.id) where shipper_table.shipper_name=%s",l)
    data = mycursor.fetchall()
    if len(data) == 0:
        return "Cannot find the name"
    else:
        return render_template("listdeliverdetails.html", data=data)

@app.route('/shipperdetails', methods=['POST','GET'])
def shipperdetails():
    mycursor = mydb.cursor()
    mycursor.execute("select S.shipper_name as 'Delivery Agent',o.Order_street as 'Delivery street' ,o.Order_city as 'Delivery city',o.Order_pincode as 'Delivery pincode' from order_table o ,shipper_table S where o.id=S.shipper_id and o.id IN (select id from order_table where TotalCost>=0)")
    data = mycursor.fetchall()
    return render_template("shipperdetails.html", data=data)

@app.route('/stockpage',methods=['POST','GET'])
def stockpage():
    return render_template("stockpage.html")

@app.route('/stock', methods=['POST','GET'])
def stock():
    name=request.form.get("name",False)
    l=[]
    l.append(name)
    mycursor = mydb.cursor()
    mycursor.execute("select Quantity,brand_table.Brand_Name from product_table natural join inventory_table natural join brand_table where product_name = %s",l)
    data = mycursor.fetchall()
    return render_template("stock.html", data=data)
##cube substitue query
@app.route('/address',methods=['POST','GET'])
def address():
    mycursor = mydb.cursor()
    mycursor.execute("""SELECT Order_city, Order_street, COUNT(*) AS Count 
                     FROM order_table 
                     GROUP BY Order_city, Order_street 
                     UNION SELECT Order_city, NULL, COUNT(*) AS Count FROM 
                     order_table  GROUP BY Order_city UNION SELECT NULL, NULL, COUNT(*) AS Count  FROM order_table""")
    data = mycursor.fetchall()
    if len(data) == 0:
        return "Cannot find the name"
    else:
        return render_template("address.html", data=data)

@app.route('/updateproduct', methods=['POST','GET'])
def updateproduct():
    return render_template("updatepage.html")

@app.route('/productlist', methods=['POST','GET'])
def productlist():
        mycursor = mydb.cursor()
        mycursor.execute("Select pt.product_id,pt.product_name,pt.product_cost,pt.brand_name,i.Quantity from inventory_table as i,product_table as pt where i.product_id=pt.product_id;")
        data = mycursor.fetchall()
        return render_template("productlist.html",data=data)
    
@app.route('/updatepro',methods=['POST', 'GET'])
def updatepro():
    id = request.form['id']
    quantity = request.form['quantity']

    if quantity == '0':
        # Insert the trigger code into the database
        try:
            mycursor = mydb.cursor()
            mycursor.execute("CREATE TRIGGER delete_product_trigger BEFORE UPDATE ON product_table FOR EACH ROW BEGIN IF NEW.Quantity = 0 THEN DELETE FROM product_table WHERE product_id = NEW.product_id; END IF; END")
            mydb.commit()
            mycursor.close()
        except mysql.connector.Error as error:
            mycursor = mydb.cursor()
            l=[]
            l.append(id)
            mycursor.execute('Delete from product_table where product_id=%s',(l))
            mydb.commit()
            mycursor.close()
            return render_template('adminhome.html')
    else:
            mycursor = mydb.cursor()
            mycursor.execute('UPDATE inventory_table SET Quantity=%s WHERE product_id=%s', (quantity, id))
            mydb.commit()
            mycursor.close()
    return render_template('adminhome.html')



@app.route('/userpage')
def userpage():
    return render_template('user.html')    
    
    
@app.route('/user', methods=['POST','GET'])
def user():
    emailid = request.form['emailid']
    password = request.form['password']
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM user_table WHERE emailid = %s AND password = %s", (emailid, password))
    data = mycursor.fetchall()
    if len(data) == 0:
        return "You are not registered"
    else:
        return render_template('userhome.html',data=data)

## two Roll up OLAP Query
@app.route('/cart', methods=['POST','GET'])
def cart():
    data_str=request.form.get("data")
    data=ast.literal_eval(data_str)
    index=[]
    index.append(data[0][0])
    totalcartquant=mydb.cursor()
    totalcartquant.execute("SELECT id, Product_Id, SUM(Quantity) FROM items_contained_table where id=%s GROUP BY id, Product_Id WITH ROLLUP",(index))
    cartquantdata=totalcartquant.fetchall()
    
    productcursor=mydb.cursor()
    productcursor.execute("select P.Product_Name,P.Brand_Name ,P.Product_Cost ,I.Quantity,P.Product_Cost*I.Quantity As Cost from product_table P,items_contained_table I where P.product_id=I.Product_ID and I.id = %s and P.Product_Id IN (select Product_Id from inventory_table where Quantity>=0)",(index))
    productdata=productcursor.fetchall()
    
    cartsummary=mydb.cursor()
    cartsummary.execute("SELECT a.id, sum(quant) ,SUM(Cost) FROM ( SELECT P.Product_Name AS 'Product Name', I.id, P.Brand_Name AS BrandName, P.Product_Cost AS 'Product Cost', I.Quantity as quant, P.Product_Cost*I.Quantity AS Cost FROM product_table P, items_contained_table I WHERE I.id=%s and P.product_id=I.Product_ID AND P.Product_Id IN (SELECT Product_Id FROM inventory_table WHERE Quantity>=0)) a GROUP BY a.id WITH ROLLUP",(index))
    cartdata=cartsummary.fetchall()
    print(cartdata)
    if (len(productdata)==0):
        return "Cart Empty"
    
    return render_template('cart.html',productdata=productdata,cartquantdata=cartquantdata,cartdata=cartdata,data=data_str)



@app.route('/addcart', methods=['POST','GET'])
def addcart():
    data_str=request.form.get("data")
    return render_template("addpage.html",data=data_str)

@app.route('/add', methods=['POST'])
def add():
    try:
        data_str = request.form.get("data")
        data = ast.literal_eval(data_str)
        index = data[0][0]

        id = request.form['id']
        quantity = int(request.form['quantity'])

        mycursor = mydb.cursor()
        delete=mydb.cursor()

        # Start transaction
        mycursor.execute("START TRANSACTION")
        
        # Acquire exclusive lock on inventory table
        mycursor.execute("SELECT * FROM inventory_table WHERE Product_Id = %s FOR UPDATE", (id,))
        inventory_data = mycursor.fetchone()
        inv_qty = inventory_data[1]

        # Check if trigger already exists
        mycursor.execute("SELECT trigger_name FROM information_schema.triggers WHERE trigger_schema = DATABASE() AND trigger_name = 'add_to_cart_trigger'")
        trigger_exists = mycursor.fetchone()

        if trigger_exists:
            # Drop trigger if it exists
            mycursor.execute("DROP TRIGGER add_to_cart_trigger")

        # Create new trigger
        mycursor.execute("""
            CREATE TRIGGER add_to_cart_trigger
            before INSERT ON items_contained_table
            FOR EACH ROW
            BEGIN
                DECLARE inv_qty INT;
                SELECT Quantity INTO inv_qty FROM inventory_table WHERE Product_Id = NEW.Product_Id;
                IF NEW.Quantity >= inv_qty THEN
                    SET NEW.Quantity = inv_qty;
                    UPDATE inventory_table SET Quantity = 0 WHERE Product_Id = NEW.Product_Id;
                ELSE
                    UPDATE inventory_table SET Quantity = inv_qty - NEW.Quantity WHERE Product_Id = NEW.Product_Id;
                END IF;
            END;
        """)

        # Insert new item into items_contained_table
        delete.execute("")
        mycursor.execute("INSERT INTO items_contained_table (Id, Product_Id, Quantity) VALUES (%s, %s, %s)", (index, id, quantity))

        # Update inventory table
        new_qty = inv_qty - quantity
        mycursor.execute("UPDATE inventory_table SET Quantity = %s WHERE Product_Id = %s", (new_qty, id))

        # Commit transaction
        mydb.commit()

        mycursor.close()

        return "Adding successful"

    except (mysql.connector.Error, ValueError, SyntaxError) as error:
        mydb.rollback()  # Rollback transaction in case of error
        mycursor.close()
        return "Error: " + str(error)
    
@app.route('/checkout', methods=['POST','GET'])
def checkout():
    data_str=request.form.get("data")
    data=ast.literal_eval(data_str)
    index=[]
    index.append(data[0][0])
    coupon=mydb.cursor()
    coupon.execute("select Coupon_Id from coupon_table where isUsed=0 AND id=%s and ExpiryDate>CURDATE()",(index))
    coupon=coupon.fetchall()
    print(coupon)
    return render_template('checkout.html',data=data,coupons=coupon)
    
@app.route('/coupon', methods=['POST','GET'])
def coupon ():
    data_str=request.form.get("data")
    data=ast.literal_eval(data_str)
    index=[]
    index.append(data[0][0])
    coup=mydb.cursor()
    coup.execute("select Coupon_Id,Discount,ExpiryDate from coupon_table where isUsed=0 AND id=%s and ExpiryDate>CURDATE()",(index))
    data=coup.fetchall()
    return render_template('coupon.html',data=data)
@app.route('/verifycoupon',methods=['POST','GET'])
def verify_coupon():
    coupon_id = request.args.get('couponID')
    data = request.form.get("data")
    query = "SELECT Coupon_Id, Discount, ExpiryDate FROM coupon_table WHERE Coupon_ID=%s AND isUsed=0 AND id= 1 AND ExpiryDate>CURDATE()"
    cursor = mysql.connection.cursor()
    cursor.execute(query, (coupon_id))
    coupon_data = cursor.fetchone()
    if coupon_data is not None:
        return "Valid coupon"
    else:
        return "Invalid coupon"
    
import traceback  
@app.route('/placeorder', methods=['POST','GET'])
def placeorder():
    # Get form dat


    try:
        data_str=request.form.get("data")
        data=ast.literal_eval(data_str)
        index=[]
        index.append(data[0][0])
        order_street=[]
        street = request.form['order_street']
        order_street.append(street)
        
        order_city=[]
        city = request.form['order_city']
        order_city.append(city)
        order_pincode=[]
        pincode = request.form['order_pincode']
        order_pincode.append(pincode)
        
        coupon_id=[]
        cid = request.form['couponID']
        coupon_id.append(cid)
        
        mycursor=mydb.cursor()
        payment_mode=[]
        pay=request.form['paymentmode']
        payment_mode.append(pay)
        
        coup=mydb.cursor()
        coup.execute("select Discount from coupon_table where isUsed=0 AND Coupon_ID=%s AND id=%s and ExpiryDate>CURDATE()",(coupon_id[0],index[0]))
        discount=coup.fetchall()
        disc=int(discount[0][0])
        print(disc)
        total_amount=[]
        cartsummary=mydb.cursor()
        cartsummary.execute("SELECT a.id, sum(quant) ,SUM(Cost) FROM ( SELECT P.Product_Name AS 'Product Name', I.id, P.Brand_Name AS BrandName, P.Product_Cost AS 'Product Cost', I.Quantity as quant, P.Product_Cost*I.Quantity AS Cost FROM product_table P, items_contained_table I WHERE I.id=%s and P.product_id=I.Product_ID AND P.Product_Id IN (SELECT Product_Id FROM inventory_table WHERE Quantity>=0)) a GROUP BY a.id WITH ROLLUP",(index))
        cartdata=cartsummary.fetchall()
        afterdisc=cartdata[-1][2]-((cartdata[-1][2]*disc)/100)
        total_amount.append(afterdisc)
        # Start the transaction
        current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # print(coupon_id,index ,payment_mode ,order_street,order_city,order_pincode,current_datetime,index,total_amount,coupon_id)
        # print((coupon_id,index ,payment_mode ,order_street,order_city,order_pincode,current_datetime,index,total_amount,coupon_id))
        mycursor.execute("""
            START TRANSACTION;
            
            -- Lock the order_table for writing
            LOCK TABLES order_table WRITE;
            
            -- Get the last order ID and unlock the table
            SELECT MAX(Order_Id) + 1 INTO @last_order_id FROM order_table;
            UNLOCK TABLES;
            
            -- Update the coupon_table to mark the coupon as used
            UPDATE coupon_table SET isUsed = 1 WHERE Coupon_Id = %s;
            
            
            -- Insert a new row into the payment_table
            INSERT INTO payment_table (payment_Id, id, isSuccess) VALUES (@last_order_id, %s, 1);
            
            -- Insert a new row into the billing_table
            INSERT INTO billing_table (Billing_id, payment_Mode) VALUES (@last_order_id, %s);
            
            -- Select a random shipper ID
            SELECT FLOOR(RAND() * (SELECT MAX(shipper_id) FROM shipper_table)) + 1 INTO @available_shipper_id;
            
            -- Insert a new row into the order_table
            
            INSERT INTO order_table (Order_Id, Order_street, Order_city, Order_pincode, Shipper_Id, Date_time, id, Billing_Id, Payment_Id, TotalCost, couponID)
            VALUES (@last_order_id, %s, %s, %s, @available_shipper_id, %s, %s, @last_order_id, @last_order_id, %s, %s);
            
            -- Commit the transaction
            DELETE FROM items_contained_table WHERE id = %s;
            DELETE FROM inventory_table WHERE Product_Id IN (SELECT Product_ID FROM items_contained_table WHERE id = %s);
            COMMIT;
                """, 
            (coupon_id[0],index[0],payment_mode[0],order_street[0],order_city[0],order_pincode[0],current_datetime,index[0],total_amount[0],coupon_id[0],index[0],index[0]))

        return render_template('successfull.html',data=data,total_amount=total_amount)
    except:
        traceback.print_exc()
        mydb.rollback()
        return render_template('unsuccessfull.html')

if __name__ == '__main__':
    app.run(debug=True)