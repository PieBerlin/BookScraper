import re
# # Define your item pipelines here
# #
# # Don't forget to add your pipeline to the ITEM_PIPELINES setting
# # See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# # useful for handling different item types with a single interface
# from itemadapter import ItemAdapter


# class BookscraperPipeline:
#     def process_item(self, item, spider):

#         adapter = ItemAdapter(item)

#         ## Strip all whitespaces from strings
#         field_names = adapter.field_names()
#         for field_name in field_names:
#             if field_name != 'description':
#                 value = adapter.get(field_name)
#                 adapter[field_name] = value[0].strip()


#         ## Category & Product Type --> switch to lowercase
#         lowercase_keys = ['category', 'product_type']
#         for lowercase_key in lowercase_keys:
#             value = adapter.get(lowercase_key)
#             adapter[lowercase_key] = value.lower()



#         # ## Price --> convert to float
#         # price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
#         # for price_key in price_keys:
#         #     value = adapter.get(price_key)
#         #     value = value.replace('£', '')
#         #     adapter[price_key] = float(value)
#         ## Price --> convert to float
#         price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
#         for price_key in price_keys:
#             value = adapter.get(price_key)
#             if value:
#                 value = value.replace('£', '')
#                 adapter[price_key] = float(value)


#         ## Availability --> extract number of books in stock
#         availability_string = adapter.get('availability')
#         split_string_array = availability_string.split('(')
#         if len(split_string_array) < 2:
#             adapter['availability'] = 0
#         else:
#             availability_array = split_string_array[1].split(' ')
#             adapter['availability'] = int(availability_array[0])



#         ## Reviews --> convert string to number
#         num_reviews_string = adapter.get('num_reviews')
#         adapter['num_reviews'] = int(num_reviews_string)


#         ## Stars --> convert text to number
#         stars_string = adapter.get('stars')
#         split_stars_array = stars_string.split(' ')
#         stars_text_value = split_stars_array[1].lower()
#         if stars_text_value == "zero":
#             adapter['stars'] = 0
#         elif stars_text_value == "one":
#             adapter['stars'] = 1
#         elif stars_text_value == "two":
#             adapter['stars'] = 2
#         elif stars_text_value == "three":
#             adapter['stars'] = 3
#         elif stars_text_value == "four":
#             adapter['stars'] = 4
#         elif stars_text_value == "five":
#             adapter['stars'] = 5


#         return item

###################################
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Strip all whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description':
                value = adapter.get(field_name)
                if value:
                    # Check if it's a list (multiple values)
                    if isinstance(value, list):
                        # Join list items or take first one
                        if len(value) > 0:
                            adapter[field_name] = value[0].strip()
                        else:
                            adapter[field_name] = ""
                    # Check if it's a tuple (from the comma issue)
                    elif isinstance(value, tuple):
                        if len(value) > 0:
                            adapter[field_name] = str(value[0]).strip()
                        else:
                            adapter[field_name] = ""
                    else:
                        # It's already a string
                        adapter[field_name] = str(value).strip()

        # Category & Product Type --> switch to lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            if value:
                if isinstance(value, list):
                    value = value[0] if len(value) > 0 else ""
                adapter[lowercase_key] = str(value).lower()

        # Price --> convert to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            if value:
                # Convert to string if it's a list/tuple
                if isinstance(value, list):
                    value = value[0] if len(value) > 0 else ""
                elif isinstance(value, tuple):
                    value = value[0] if len(value) > 0 else ""
                
                value = str(value).replace('£', '')
                # Remove currency symbol and non-numeric characters
                cleaned = re.sub(r'[^\d\.]', '', value)
                try:
                    if cleaned:
                        adapter[price_key] = float(cleaned)
                    else:
                        adapter[price_key] = 0.0
                except ValueError:
                    spider.logger.warning(f"Could not convert {price_key} value '{value}' to float")
                    adapter[price_key] = 0.0

        # Availability --> extract number of books in stock
        availability_string = adapter.get('availability')
        if availability_string:
            if isinstance(availability_string, list):
                availability_string = availability_string[0] if len(availability_string) > 0 else ""
            elif isinstance(availability_string, tuple):
                availability_string = availability_string[0] if len(availability_string) > 0 else ""
            
            split_string_array = str(availability_string).split('(')
            if len(split_string_array) < 2:
                adapter['availability'] = 0
            else:
                availability_array = split_string_array[1].split(' ')
                try:
                    adapter['availability'] = int(availability_array[0])
                except (ValueError, IndexError):
                    adapter['availability'] = 0

        # Reviews --> convert string to number
        num_reviews_string = adapter.get('num_reviews')
        if num_reviews_string:
            if isinstance(num_reviews_string, list):
                num_reviews_string = num_reviews_string[0] if len(num_reviews_string) > 0 else "0"
            elif isinstance(num_reviews_string, tuple):
                num_reviews_string = num_reviews_string[0] if len(num_reviews_string) > 0 else "0"
            
            try:
                adapter['num_reviews'] = int(str(num_reviews_string))
            except ValueError:
                adapter['num_reviews'] = 0

        # Stars --> convert text to number
        stars_string = adapter.get('stars')
        if stars_string:
            if isinstance(stars_string, list):
                stars_string = stars_string[0] if len(stars_string) > 0 else ""
            elif isinstance(stars_string, tuple):
                stars_string = stars_string[0] if len(stars_string) > 0 else ""
            
            stars_string = str(stars_string)
            if 'One' in stars_string:
                adapter['stars'] = 1
            elif 'Two' in stars_string:
                adapter['stars'] = 2
            elif 'Three' in stars_string:
                adapter['stars'] = 3
            elif 'Four' in stars_string:
                adapter['stars'] = 4
            elif 'Five' in stars_string:
                adapter['stars'] = 5
            else:
                adapter['stars'] = 0

        return item

import mysql.connector 
class SaveToMySQLPipeline:

    def __init__(self):
        self.conn = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            password = 'Rackie@3375', #add your password here if you have one set 
            database = 'books'
        )

        ## Create cursor, used to execute commands
        self.cur = self.conn.cursor()

        ## Create books table if none exists
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS books(
            id int NOT NULL auto_increment, 
            url VARCHAR(255),
            title text,
            upc VARCHAR(255),
            product_type VARCHAR(255),
            price_excl_tax DECIMAL,
            price_incl_tax DECIMAL,
            tax DECIMAL,
            price DECIMAL,
            availability INTEGER,
            num_reviews INTEGER,
            stars INTEGER,
            category VARCHAR(255),
            description text,
            PRIMARY KEY (id)
        )
        """)

    def process_item(self, item, spider):
        # --- NEW SAFE HANDLING FOR DESCRIPTION ---
        description_value = item.get("description")
        
        if description_value:
            # Check if it's a list (which it likely is from Scrapy extraction)
            if isinstance(description_value, list) and len(description_value) > 0:
                # Get the first element and convert to string safely
                safe_description = str(description_value) 
            else:
                # It might already be a single string if processed elsewhere
                safe_description = str(description_value)
        else:
            # If description is missing entirely, use an empty string or None (if your DB allows nulls)
            safe_description = "" 
        # ----------------------------------------

        ## Define insert statement
        self.cur.execute(""" insert into books (
            url, 
            title, 
            upc, 
            product_type, 
            price_excl_tax,
            price_incl_tax,
            tax,
            price,
            availability,
            num_reviews,
            stars,
            category,
            description
            ) values (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
                )""", (
            item["url"],
            item["title"],
            item["upc"],
            item["product_type"],
            item["price_excl_tax"],
            item["price_incl_tax"],
            item["tax"],
            item["price"],
            item["availability"],
            item["num_reviews"],
            item["stars"],
            item["category"],
            str(item["description"][0])
        ))

        # ## Execute insert of data into database
        self.conn.commit()
        return item

    
    def close_spider(self, spider):

        ## Close cursor & connection to database 
        self.cur.close()
        self.conn.close()