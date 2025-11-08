# Data model for 3 sprint

# модель данных источников
## customer_research 
данные маркетинговых исследований
date_id - дата исследования в формате YYYYMMDD (int) 
category_id - идентификатор катогории товара (int)
geo_id - идентификатор города проведения исследования (int) 
sales_qty - продажи, ед. товара (bigint)
sales_amt - продажи в условной валюте (decimal(10,2))	

## user_order_log
логи активности пользователей
date_time - дата-время активности (timestamp)
city_id - идентификатор город клиента (int)
city_name - название города клиента (varchar(50)) 
customer_id - идентификатор клиента (int)
first_name - имя клиента (varchar(50))
last_name - фамилия клиента (varchar(50))
item_id - идентификатор товара (int) 
item_name - название продукта (varchar(50))
quantity - количество товаров (decimal(10,2))
payment_amount - сумма платежа в рублях (decimal(10, 2))

## user_activity_log
логи активности пользователей
date_time - дата-время активности (timestamp)
action_id - идентификатор активности пользователя (int)
customer_id - идентификатор клиента (int) 
quantity - количество кликов

## price_log
информация о цене продукта на дату
datetime - дата-время начала действия цены
category_id - идентификатор категории продукта (int)
category_name - название категории продукта (varchar(50))
item_id - идентификатор товара (int)
price - значение цены в рублях (decimal(10,2))



# модель данных ВАКИ
## d_customer
иерархия клиентов
customer_id - идентификатор клиента  (int)
first_name - имя клиента (varchar(50))
last_name - фамилия клиента (varchar(50))
сity_id - идентификатор город клиента (int)

## d_city
список городов 
сity_id - идентификатор город клиента (int)
city_name - название города клиента (varchar(50))   

## d_item - продукты
item_id - идентификатор продукта  (int)
item_name - название продукта (varchar(50)
category_id - идентификатор категории продукта (int)  

## d_category - категория продукта
category_id - идентификатор категории продукта (int)
category_name - название категории продукта (varchar(50))

## d_calendar - календварь  
date_id - идентификатор дня (int)
day_num - номар дня (tinyint)
month_num - номер месяца (tinyint)
month_name - название месяца (varchar(8))
year_num - год (tinyint)

## f_activity
таблица фактов активности на сайте 
activity_id - идентификатор активности (int)
date_id - дата автивности в формате YYYYMMDD (int)
click_number - количество кликов (longint)

## f_daily_sales
таблица фактов продаж (по activity_id = ...) 
date_id - дата продаж в формате YYYYMMDD (int)
item_id - идентификатор продукта  (int)
customer_id - идентификатор клиента  (int)
price - цена товара из прайс листа (double(10,2)) 
quantity - количество купленного товара (double(10,2))
amount - сумма покупки (double(10,2))

## f_research
таблица фактов исследования рынка 
date_id - дата продаж в формате YYYYMMDD (int)
category_id - идентификатор продукта  (int)
geo_id - идентификатор клиента  (int)
quantity - количество купленного товара (double(10,2))
amount - сумма покупки (double(10,2))

