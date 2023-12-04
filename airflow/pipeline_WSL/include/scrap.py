# ------------------------------------------------------------class1
# Class of web page
class w_page():
        def __init__(self, desc, url):
            self.__desc=desc
            self.__url=url
        
        @property
        def desc(self): return self.__desc

        @property
        def url(self): return self.__url

        @property
        def info(self): return f'{self.__desc}, {self.__url}'
# ----------------------------------------------------------------------------------------------class2
import os
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
# import class1_wp_home as c1
import sys
# sys.stdout.reconfigure(encoding='utf-8')

# A class of the search web page, that inherits from the class of the home page 
# The inheritance is used for educational purposes
class wpage_links(w_page):
    def __init__(self, desc, url):
        super().__init__(desc, url)            

    # Decorator for viewing self.desc + self.url properties  
    # Decorator is used for educational purposes     
    @property 
    def info(self): return f'desc={self.desc} url={self.url}'

    
    def get_links(self, count_page): 
        """
        Collects all ad links from the main search page. 
        The number of pages can be limited - the "count_page" parameter

        :return: File web_urls_.json with list of links/
        """  
            # log message
        tmf = r"%d/%m/%Y %H:%M:%S" # time format for message
        fpath = os.path.dirname(os.path.abspath(__file__))
        print(datetime.now().strftime(tmf),"=", 'Scrapping links is started...') 

        self.count_page = count_page      
        # Send an HTTP GET request
        lst_urls    = list()
        var_i       = 1    # the number of this link
        var_ii      = 1    # the number of the web page with link od ad 
    
        url = f"{self.url}=BE&page={var_ii}&orderBy=relevance"
        response = requests.get(url)
        var_respcode=response.status_code
        while var_respcode == 200 and var_ii<count_page+1: 
            
            # Parsing the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')

            # Finding all links
            soupF = soup.find_all("a", class_="card__title-link") 

            # Extracting and saving the links one-by-one
            var_x = 1
            for e in soupF:        
                if e:
                    href = e.get("href") 
                    lst_urls.append({"N":var_i, "Nch":0, "Np":var_ii, "url":href})
                    if var_i%1000==0: 
                        print(datetime.now().strftime(tmf),"=", var_i, 'links are collected')
                    var_x+=1
                    var_i+=1
            var_ii+=1
            # next while           
            url = f"{self.url}=BE&page={var_ii}&orderBy=relevance"
            response = requests.get(url)
            var_respcode=response.status_code
            # print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
            
        #
        var_json = json.dumps(lst_urls, indent=2, ensure_ascii=False) #indent=2 >> pretty-printing      
        with open(fpath + '/web_urls_1.json', "w", encoding='utf-8') as var_jf:
            var_jf.write(var_json) 

            # log message
        time=datetime.now().strftime(tmf)    
        print(f"{time} = Scrapping links is finished. {var_i-1} links are saved to json.")
        return var_i-1 # count collected urls
      
# ------------------------------------------------------------class3
import sys
import json
import requests
from bs4 import BeautifulSoup
import os
# import class2_wp_links as c2 #utils.
# sys.stdout.reconfigure(encoding='utf-8')

# A class of the web page of a real estate object, that inherits from the class of the search page 
# The inheritance is used for educational purposes
class wpage_property(wpage_links):
    def __init__(self, desc, url, N, Nch):
        super().__init__(desc, url)
        self.N = int(N)
        self.Nch = int(Nch)
        self.ddict = {}

    def get_data_page(self): # rename from "get_data_immoweb"
        """
        A function that collects the data of this real estate ad according to the web address 
        of the "url" property of this class object

        :return: writes data to the "ddict" property of this class object - 
        (fills this dictionary with data)
        """  
        #
        fpath = os.path.dirname(os.path.abspath(__file__))       
        r = requests.get(self.url)
        #
        s = BeautifulSoup(r.text, "lxml")        
        script_tag = s.find('script', type='text/javascript') # get info between "script" tags 
        script_txt = script_tag.string  # Get the text content from the <script> tag        
        jtext_A = script_txt.find('{') # finding start position of content
        jtext_Z = script_txt.rfind('}')  # finding end position of content    
        jtext = script_txt[jtext_A:jtext_Z+1] # get the row text of java dict
        if jtext[2:4] != 'id': return False # if "id" not exist on the right place, stop running            
        self.row_data = json.loads(jtext) # convert row text to python dict <class 'dict'>
            # debug
            # json_dbg = "link number = {self.no}" 
            # print(type(json_dbg))
        json_dbg  = '.\nThe row text of last processed web page.'
        json_dbg += f'\nLink number:{self.N}\n'
        json_dbg += json.dumps(self.row_data, indent=2, ensure_ascii=False) #indent=2 >> fpath = os.fpath = path.dirname(os.path.abspath(__file__))
        with open(fpath + "/web_row_data.json", "w", encoding='utf-8') as v_jf: 
            v_jf.write(json_dbg) 
    # ---------------------------------
        if self.row_data["property"]["type"] in ["APARTMENT_GROUP","HOUSE_GROUP"]:  
            lst_urls2=[]
            soupF = s.find_all('a', class_='classified__list-item-link')
            
            # Extracting and saving the links of the group ad links
            var_x=1
            for e in soupF:        
                if e:
                    href = e.get("href")
                    # print(var_i, var_ii, var_x, href)
                    # noo = str(self.Nch) #+ "/" + str(var_x)
                    lst_urls2.append({"N":self.N, "Nch":var_x, "url":href})
                    var_x+=1  
            var_json = json.dumps(lst_urls2, indent=2, ensure_ascii=False) #indent=2 >pretty-printing   
            with open(fpath + '/web_urls_2.json', "a", encoding='utf-8') as var_jf:
                var_jf.write(var_json) 

    # ---------------------------------

        self.ddict["Nch"]  = self.Nch
        self.ddict["N"]   = self.N
        self.ddict["url"] = self.url


        try: # --- id
            t = self.row_data["id"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["id"] = t


        try: # --- Locality_Name       
            t = self.row_data["property"]["location"]["locality"]           
        except (KeyError, TypeError):
           t = None
        self.ddict["Locality_Name"] = t


        try: # --- Postal_code
            t = self.row_data["property"]["location"]["postalCode"]           
        except (KeyError, TypeError):
            t = None
        self.ddict["Postal_code"] = t


        try: # --- Type_of_property
            t = self.row_data["property"]["type"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["Type_of_property"] = t


        try: # --- Subtype_of_property
            t   = self.row_data["property"]["subtype"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["Subtype_of_property"] = t


        try: # --- Price
            t = self.row_data["transaction"]["sale"]["price"]           
        except (KeyError, TypeError):
            t = None
        self.ddict["Price"] = t


        try: # --- Type_of_sale
            t  = self.row_data["transaction"]["type"]            
        except (KeyError, TypeError):
            t= None
        self.ddict["Type_of_sale"] = t


        try: # --- Number_of_rooms
            t = self.row_data["property"]["bedroomCount"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["Number_of_rooms"]=t            


        try: # --- Living_Area
            t = self.row_data["property"]["netHabitableSurface"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["Living_Area"]=t


        try: # --- Equipped_kitchen
            t = self.row_data["property"]["kitchen"]["type"]            
        except (KeyError, TypeError):   
            t = None
        self.ddict["Equipped_kitchen"] = t
        
        
        try: # --- Furnished   
            t = str(self.row_data["transaction"]["sale"]["isFurnished"])
        except (KeyError, TypeError):   
            t = None
        self.ddict["Furnished"] = t
     
        
        try: # --- Open_fire   
            t = str(self.row_data["property"]["fireplaceExists"])
        except (KeyError, TypeError):   
            t = None
        self.ddict["Open_fire"] = t

        
        try: # --- Terrace_YN   
            t = str(self.row_data["property"]["hasTerrace"])
        except (KeyError, TypeError):   
            t = None
        self.ddict["Terrace_YN"] = t


        try: # --- TerraceSurface
            t = self.row_data["property"]["terraceSurface"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["TerraceSurface"] = t


        try: # -- Garden_YN
            t = self.row_data["property"]["hasGarden"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["Garden_YN"] = t


        try: # --- Garden_area
            t = self.row_data["property"]["gardenSurface"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["Garden_area"] = t


        try: # --- Surface_of_good
            t = self.row_data["property"]["building"]["streetFacadeWidth"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["Surface_of_good"]=t


        try: # -- Number_of_facades
            t = self.row_data["property"]["building"]["facadeCount"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["Number_of_facades"] = t


        try: # -- Swimming_pool_YN
            t = self.row_data["property"]["hasSwimmingPool"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["Swimming_pool_YN"] = t


        try: # -- State_of_building
            t = self.row_data["property"]["building"]["condition"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["State_of_building"] = t

        # -------------------------------------------- additional info
        try: # -- ipiNo
            t = self.row_data["customers"][0]["ipiNo"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["ipiNo"] = t

        try: # -- customer_type
            t = self.row_data["customers"][0]["type"]            
        except (KeyError, TypeError):
            t = None
        self.ddict["customer_type"] = t

        try: # -- regionCode
            t = self.row_data["property"]["location"]["regionCode"]         
        except (KeyError, TypeError):
            t = None
        self.ddict["regionCode"] = t

        try: # -- location region
            t = self.row_data["property"]["location"]["region"]         
        except (KeyError, TypeError):
            t = None
        self.ddict["region"] = t

        try: # -- location province
            t = self.row_data["property"]["location"]["province"]         
        except (KeyError, TypeError):
            t = None
        self.ddict["province"] = t

        try: # -- location_type
            t = self.row_data["property"]["location"]["type"]         
        except (KeyError, TypeError):
            t = None
        self.ddict["location_type"] = t

        try: # -- seller_type-------------------------------+
            t = self.row_data["customers"][0]["type"]             
        except (KeyError, TypeError):
            t = None
        self.ddict["seller_type"] = t
        
        try: # -- seller_name
            t = self.row_data["customers"][0]["name"]             
        except (KeyError, TypeError):
            t = None
        self.ddict["seller_name"] = t

        try: # --seller_website
            t = self.row_data["customers"][0]["website"]             
        except (KeyError, TypeError):
            t = None
        self.ddict["seller_website"] = t


        return True
# ------------------------------------------------------class4    
import sys
import json
from datetime import datetime
# import class3_wp_property as c3 
# sys.stdout.reconfigure(encoding='utf-8')

class Scrapper():
    pass

    def proc_links(self, coumter, file_json, limit_links=None):
        """
        A function that reads links from the specified json file and starts processing links one by one

        :return: list of dictionaries with information from the web page. One dictionary is returned for each json link. 
        If there are 10 links in the json file, it will return one list with 10 dictionaries
        """      
        # open json file with ordinary links to (jurls) list
        tmf=r"%d/%m/%Y %H:%M:%S" # time format for message
        print( datetime.now().strftime(tmf) +" = Processing links from " + file_json )
        lst, fpath = [], os.path.dirname(os.path.abspath(__file__))
        
        try:
            with open(fpath +'/'+file_json, encoding='utf-8') as f: 
                fj = f.read().replace('][', ',')              
                jurls = json.loads(fj)
            # link by limk 
# ---------------------------------------------------------------------------
            for u in (jurls[:limit_links]): # for DEBUG set replace "jurls" to "jurls[:10]"
                p = wpage_property( "obj"+str(u['Nch']), u["url"], N=u['N'], Nch=u['Nch'])  # create immo objects 
                if p.get_data_page()==True: # if page handling is succeeded      
                    #message every 100 processed links
                    if int(coumter)%100==0:          
                        log_msg = datetime.now().strftime(tmf) +" = " + str(coumter) + ' links are processed'
                        print(log_msg)            
                else:  # if page handling is NOT succeeded, massege about bad link and continue     
                        print(datetime.now().strftime(tmf) +" = " + str(p.N) + ' bad link')
                        continue
                lst.append(p.ddict) # adding data page in dict for recording
                coumter+=1
            log_msg = datetime.now().strftime(tmf) +" = Processing link from" + file_json + " finished." 
            return lst

        except json.JSONDecodeError: 
            print(datetime.now().strftime(tmf) +" = Processing links from " + file_json + " finished.")       
            return lst
        



# -----------------------------------------------------main

  
import csv
import os
from datetime import datetime
import sys
# sys.stdout.reconfigure(encoding='utf-8')

def scrapper_run(pages_limit=500, limks_limit=None):        
    """---------------------------------
    ------- START SCRAPPING ------------
    ---------------------------------"""

    desc,url1       = "immoweb","https://www.immoweb.be"
    url2            = "/en/search/house-and-apartment/for-sale"
    obj_wpage_home  = w_page(desc,url1) 
    ooj_wpage_links = wpage_links("webpage with ad links", obj_wpage_home.url + url2) 
    obj_scr         = Scrapper()
    fpath           = os.path.dirname(os.path.abspath(__file__))
    # pages_for_scrap = 5     # normally = 500 ++  line 26

    # clear file with group links
    fpath = os.path.dirname(os.path.abspath(__file__))    
    with open(fpath + '/web_urls_data.csv', 'w', encoding='utf-8') as f: f.write('')
   

    # getting standart links, save them to json-file and get count scrapped links
    ooj_wpage_links.get_links(pages_limit) 

    # Starting link processing and saving data to the list of dictionaries    
    y = list()
    y = obj_scr.proc_links(1, "web_urls_1.json",limks_limit) # json 1
    y +=obj_scr.proc_links(1, "web_urls_2.json",limks_limit) # json 2 - adding to y


    # preparing save dict to CSV
        # get headers 
    field_names = y[0].keys() # get all keys od dict for csv.DictWriter
        # writing dict to CSV
    file1 = fpath + '/web_urls_data.csv'
    file2 = fpath + '/.web_urls_data_'+datetime.now().strftime("%Y%m%d-%H%M%S")+'.csv'    
    for ffl in [file1,file2]:
        with open(ffl, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(y)

# datetime.now().strftime("%Y%m%d-%H%M%S")

    log_message = datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "= The scrapping is fifnished."    
    return log_message

# run
m = scrapper_run(pages_limit = 2, limks_limit = 20) 
print(m)