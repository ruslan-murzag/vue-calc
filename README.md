# vue-calc

def cat_price(file, records){
    if (records.size() < 3){
        return null
    }

   
    withPool {
	   
        de localRecords = records[2..-1]

        localRecords.eachParallel {
	  	def productId
    	def city
    	def price
    	def discount
    	def available
    	def delivery
	  	def message = [:]

            it = it.join('','').split('';'')
            if(it.size() > 3){
                productId = it[0]
                city = it[1]
                price = it[2]
                if(it.size() >= 4){
                    if (file =~ ''goldapple''){
                        if(it[3] == 0){
                            discount = 0
                        }else {
                            discount = Math.abs(price.toInteger() - it[3].toInteger())
                        }
                    } else if (file =~ ''GlobalWildberries'' && it.size() >= 5){

                        available = it[3]
                        delivery = it[4]
                    }else {
                        available = it[3]
                    }
                }
            }
            message.put(''productId'', productId)
            message.put(''availability'', available)
            message.put(''price'', price)
            message.put(''discount'', discount)
            message.put(''delivery'', delivery)
            message.put(''city'', city)
            message.put(''time_stamp'', sdf.format(now))
            MemUtils.sendMessage(''KAFKA_PROD_NEW'', ''IN.S0109.WEB_E_MODEL_PRICES'', false, new JsonBuilder(message).toString())
            message = [:]
        }
    }
}


def cat_cities(file, records){


    if (records.size() > 2){
        withPool {
            def localrecords = records[2..-1]
            localrecords.eachParallel {
	  		    def db_id
    			def id
    			def shop_code
    			def city
	  	  		def message = [:]

                it = it.join('','').split('';'')
                if (it.size() > 3){
                    db_id = it[0]
                    id = it[1]
                    shop_code = it[2]
                    city = it[3]
                }
                message.put(''cityId'', id)
                message.put(''city'', city)
                message.put(''shopName'', shop_code)
                message.put(''time_stamp'', sdf.format(now))
                MemUtils.sendMessage(''KAFKA_PROD_NEW'', ''IN.S0109.WEB_E_SHOP_CITIES'', false, new JsonBuilder(message).toString())

                message = [:]
            }
        }
    }
}

def cat_cat(file, records){

    if (records.size() >2) {
        def localRecords = records[2..-1]
        withPool {
            localRecords.eachParallel {
	  			def db_id
    			def id
    			def parent_id
    			def shop_code
    			def category
    			def url
	  		    def message = [:]

                it = it.join('','').split('';'')
                if (it.size() >= 6) {
                    db_id = it[0]
                    id = it[1]
                    parent_id = it[2]
                    shop_code = it[3]
                    category = it[4]
                    url = it[5]
                }else if (it.size() == 5) {
                    db_id = it[0]
                    id = it[1]
                    parent_id = it[2]
                    shop_code = it[3]
                    category = it[4]
                }
                message.put(''catId'', id)
                message.put(''parentId'', parent_id)
                message.put(''shopName'', shop_code)
                message.put(''name'', category)
                message.put(''url'', url)
                message.put(''time_stamp'', sdf.format(now))
                MemUtils.sendMessage(''KAFKA_PROD_NEW'', ''IN.S0109.WEB_E_CATS'', false, new JsonBuilder(message).toString())
                message = [:]
            }
        }
    }
}


def cat_site(file, records){

    if (records.size() > 2){
        def localRecords = records[2..-1]
        withPool {
            localRecords.eachParallel {
	  			def id
    			def shop_code
    			def category
    			def product_id
    			def product_name
    			def url
    			def button
	  	  		def message = [:]

                it = it.join('','').split('';'')
                if (it.size() >= 7) {
                    id = it[0]
                    shop_code = it[1]
                    category = it[2]
                    product_id = it[3]
                    product_name = it[4]
                    url = it[5]
                    button = it[6]
                } else if(it.size() == 6){
                    id = it[0]
                    shop_code = it[1]
                    category = it[2]
                    product_id = it[3]
                    product_name = it[4]
                    url = it[5]
                }
                message.put(''id'', id)
                message.put(''shopName'', shop_code)
                message.put(''catId'', category)
                message.put(''productId'', product_id)
                message.put(''name'', product_name)
                message.put(''url'', url)
                message.put(''ks_sku'', button)
                message.put(''time_stamp'', sdf.format(now))

            MemUtils.sendMessage(''KAFKA_PROD_NEW'', ''IN.S0109.WEB_E_MODELS'', false, new JsonBuilder(message).toString())
            }
        }
    }
}

def main(){
    def folders = xml_parser("/remote.php/dav/files/Nazgul.Kurganbekova@kaspi.kz/Kaspi.kz/")[1..-1]

    withPool {
        folders.eachParallel{
            folder = it
            println(folder)
            def url_folder = folder.toString() + ''CSV/''
            def files
            try {
                files = xml_parser(url_folder)

            }
            catch (e){
                files = null
            }
            if (files != null && files.size() > 3 ){
                counter = 1
                l = files.size() - 1
                counter = 4
                while (counter > 0){
                    file = files[l]
                    if (file =~ ''Readme''){
                        l -= 1
                        continue
                    }
                    records = zip_unpacker(file)
                    switch (counter){
                        case 4:
                            if (file =~ ''samatshow'' || file =~ ''oksport''){
                                counter -= 1
                                continue
                            }
                            cat_price(file, records)
                            break
                        case 3:
                            cat_cities(file, records)
                            break
                        case 2:
                            if (records.size() > 2){
                                cat_cat(file, records)
                            }
                            break
                        case 1:
                            if (file =~ ''samatshow'' || file =~ ''oksport''){
                                counter -= 1
                                continue
                            }
                            cat_site(file,records)


                    }



                    l -= 1
                    counter -= 1
                }
            }
        }

    }
}
