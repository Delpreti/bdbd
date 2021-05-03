""" Test file for formulite """
import asyncio
from modules.formulite import formulite
from modules.mySoup import seed
import requests

async def initialize(manager):
    manager.set_clear()

    manager.set_entity("Produto", prod_id="INT", prod_name="TEXT", prod_spec="TEXT")
    manager.set_primary_key("Produto", "prod_id")

    manager.set_entity("Loja", l_nick="TEXT", l_name="TEXT", l_credit="BOOL", l_delivery="BOOL", l_address="TEXT")
    manager.set_primary_key("Loja", "l_nick")

    manager.set_entity("Anuncio", l_nick="TEXT", prod_id="INT", prod_price="FLOAT", time_catch="TEXT")
    manager.set_primary_key("Anuncio", "l_nick", "prod_id", "prod_price")
    manager.set_foreign_key("Anuncio", "l_nick", "Loja")
    manager.set_foreign_key("Anuncio", "prod_id", "Produto")

    await manager.create_tables()

async def scrap(manager, pid):

    prod_dict = {}
    anun_dict = {}
    loja_dict = {}

    prod_dict["prod_id"] = pid
    link = "https://www.boadica.com.br/produtos/p" + str(prod_dict["prod_id"])
    pagina = requests.get(link)
    tree_list = seed.build(string=pagina.text, below_class="col-md-8")
    if not tree_list:
        return
    #print(f"{len(tree_list)} Trees were generated.")
    resultado = tree_list[0]
    none_check = resultado.find_class("nome")
    if not none_check:
        return
    prod_dict["prod_name"] = resultado.find_class("nome").get("text").strip()
    prod_dict["prod_spec"] = resultado.find_class("especificacao").get("text").strip()
    # Produto OK
    
    anun_dict["prod_id"] = prod_dict["prod_id"]
    advance = resultado.find_class("row painel")
    #advance.find_class("data-hora").show()
    anun_dict["time_catch"] = advance.find_class("data-hora").get("text").strip()
    advance = advance.find_class("tab-content")

    lojas = advance.find_all_class("row")
    if len(lojas) > 0:
        for loja in lojas:
            anun_dict["prod_price"] = loja.find_class("col-md-3 preco-loja").get("text").strip().replace("\n\r                                                        \n", "")
            #loja.find_class("col-md-3 preco-loja").show()
            loja_dict["l_nick"] = loja.find(tagname="a").get("href").split("/")[-1]
            anun_dict["l_nick"] = loja_dict["l_nick"]
            loja_dict["l_name"] = loja.find(tagname="a").get("text").strip()
            loja_dict["l_credit"] = len(loja.find_all_class("fa fa-credit-card")) > 0
            loja_dict["l_delivery"] = len(loja.find_all_class("fa fa-motorcycle")) > 0
            pagina_loja = requests.get("https://www.boadica.com.br/loja/" + loja_dict["l_nick"])
            tree_list_2 = seed.build(string=pagina_loja.text, below_class="container")
            #print(f"{len(tree_list_2)} Trees were generated.")
            if not tree_list_2:
                return
            res = tree_list_2[0]
            linfo = res.find_class("col-md-12")
            spam = linfo.find_all(tagname="span")
            info_string = []
            for span in spam:
                info_string.append(span.get("text"))
            loja_dict["l_address"] = info_string[5].strip().replace("Endereço:\n\n\r                \n", "")
            #lphone = info_string[-3].strip() # remover caractere '/' se houver
            await manager.build_and_insert("Loja", **loja_dict)
            await manager.build_and_insert("Anuncio", **anun_dict)
        await manager.build_and_insert("Produto", **prod_dict)

async def scrap_all(manager):
    for i in range(162000, 162002):
        await scrap(manager, i)

async def main():

    manager = await formulite.manager()

    if not manager.loaded():
        await initialize(manager)

    await scrap_all(manager)

    produtos = await manager.select_from("Produto")
    print(len(produtos))

    await manager.close()

asyncio.run(main())
