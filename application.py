import asyncio
from modules.formulite import formulite
from modules.formuliteutils import utils as f_utils
from modules.mySoup import seed
import requests
import re

async def initialize(manager):
    manager.set_clear_all()

    manager.set_entity("Produto", prod_id="INT", prod_name="TEXT", prod_spec="TEXT")
    manager.set_primary_key("Produto", "prod_id")

    manager.set_entity("Loja", l_nick="TEXT", l_name="TEXT", l_credit="BOOL", l_delivery="BOOL", l_address="TEXT")
    manager.set_primary_key("Loja", "l_nick")

    manager.set_entity("Anuncio", l_nick="TEXT", prod_id="INT", prod_price="FLOAT", time_catch="TEXT")
    manager.set_primary_key("Anuncio", "l_nick", "prod_id", "prod_price")
    manager.set_foreign_key("Anuncio", "l_nick", "Loja")
    manager.set_foreign_key("Anuncio", "prod_id", "Produto")

    manager.set_entity("Meta", metakey="INT", range_start="INT", range_end="INT")
    manager.set_primary_key("Meta", "metakey")

    await manager.create_tables()

async def init_meta(manager):
    # initialize the meta table
    meta_dict = {}

    meta_dict["metakey"] = 1
    meta_dict["range_start"] = 161000
    meta_dict["range_end"] = meta_dict["range_start"] + 1

    await manager.build_and_insert("Meta", **meta_dict)

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
    resultado = tree_list[0]
    none_check = resultado.find_class("nome")
    if not none_check:
        return
    prod_dict["prod_name"] = resultado.find_class("nome").get("text").strip()
    prod_dict["prod_spec"] = resultado.find_class("especificacao").get("text").strip()
    
    anun_dict["prod_id"] = prod_dict["prod_id"]
    advance = resultado.find_class("row painel")
    anun_dict["time_catch"] = advance.find_class("data-hora").get("text").strip()
    advance = advance.find_class("tab-content")

    lojas = advance.find_all_class("row")
    if len(lojas) > 0:
        for loja in lojas:
            anun_dict["prod_price"] = loja.find_class("col-md-3 preco-loja").get("text").strip().replace("\n\r                                                        \n", "")
            loja_dict["l_nick"] = loja.find(tagname="a").get("href").split("/")[-1]
            anun_dict["l_nick"] = loja_dict["l_nick"]
            loja_dict["l_name"] = loja.find(tagname="a").get("text").strip()
            loja_dict["l_credit"] = len(loja.find_all_class("fa fa-credit-card")) > 0
            loja_dict["l_delivery"] = len(loja.find_all_class("fa fa-motorcycle")) > 0
            pagina_loja = requests.get("https://www.boadica.com.br/loja/" + loja_dict["l_nick"])
            tree_list_2 = seed.build(string=pagina_loja.text, below_class="container")
            if not tree_list_2:
                return
            res = tree_list_2[0]
            linfo = res.find_class("col-md-12")
            spam = linfo.find_all(tagname="span")
            info_string = []
            for span in spam:
                info_string.append(span.get("text"))
            loja_dict["l_address"] = info_string[5].strip().replace("Endereço:\n\n\r                \n", "")
            await manager.build_and_insert("Loja", **loja_dict)
            await manager.build_and_insert("Anuncio", **anun_dict)
        await manager.build_and_insert("Produto", **prod_dict)

async def scrap_some(manager, amount):

    meta_rows = await manager.select_all_from("Meta")
    if not meta_rows:
        return
    meta_object = meta_rows[0]

    for i in range(meta_object.range_end, meta_object.range_end + amount):
        await scrap(manager, i)

    meta_object.range_end += amount

    await manager.update(meta_object)

def get_price(anuncio_obj):
    encontro = re.search("R\$ (\d+,\d+)", anuncio_obj.prod_price)
    if encontro:
        return float( encontro.group(1).replace(",", ".") )
    else:
        return None

async def main():

    manager = await formulite.manager()

    if not manager.loaded():
        await initialize(manager)
        await init_meta(manager)

    while True:
        opcao = int( input("Insira um número:\n1 - Pesquisar itens da web\n2 - Acessar BD\n3 - Sair\n") )
        if opcao == 3:
            print("Até mais")
            break
        if opcao == 1:
            n = int( input("Quantos elementos deseja pesquisar: ") )
            print("Aguarde um momento...")
            before = await manager.count("Produto")
            await scrap_some(manager, n)
            after = await manager.count("Produto")
            print(f"{n} itens pesquisados, {after - before} novos itens foram encontrados.")
        if opcao == 2:
            info = int( input("Selecione a informação desejada:\n1 - Quantidade de elementos no banco\n2 - Produto mais barato\n3 - Lojas localizadas no Ed. Central\n4 - Lojas que realizam delivery\n") )
            if info == 1:
                num_produtos = await manager.count("Produto")
                num_lojas = await manager.count("Loja")
                num_anuncios = await manager.count("Anuncio")
                print(f"{num_produtos} produtos, {num_lojas} lojas e {num_anuncios} anúncios")
            if info == 2:
                anuncios = await manager.select_all_from("Anuncio")
                if not anuncios:
                    print("O banco está vazio!")
                else:
                    least = anuncios[0]
                    for anuncio in anuncios:
                        if get_price(anuncio) and get_price(least):
                            if get_price(anuncio) < get_price(least):
                                least = anuncio
                    prod = await manager.select_all_from("Produto", f_utils.where(prod_id=least.prod_id))
                    store = await manager.select_all_from("Loja", f_utils.where(l_nick=least.l_nick))
                    print(f"O produto produto mais barato é o(a) {prod[0].prod_name} vendido a {least.prod_price} pela loja {store[0].l_name}.")
            if info == 3:
                lojas = await manager.select_all_from("Loja")
                l_central = []
                for l in lojas:
                    verif = re.search("Av[\.]?(enida)? Rio Branco[\,]? 156", l.l_address)
                    if verif:
                        l_central.append(verif)
                print(f"De {len(lojas)} lojas, {len(l_central)} estão localizadas no Edifício Central.")
            if info == 4:
                lojas_total = await manager.select_all_from("Loja")
                lojas_delivery = await manager.select_all_from("Loja", f_utils.where(l_delivery=1))
                print(f"De {len(lojas_total)} lojas, {len(lojas_delivery)} fazem delivery.")

    await manager.close()

asyncio.run(main())
