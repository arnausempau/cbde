import random
import datetime as dt
from neo4j import GraphDatabase


key = ["1", "2", "3", "4", "5"]
brand = ["Bultaco", "Nike", "Balenciaga", "Uma", "Hilfiger"]
address = ["Barcelona", "Sevilla", "Bélgica", "Moscú", "Washington"]
nation = ["España", "Germany", "Rusia", "Estados Unidos"]
region = ["Europa", "Asia", "América"]
date = ["2020-02-01", "2020-12-23", "2003-10-02", "2021-04-24", "2020-02-02"]
priority = ["Alta", "Baja", "Media", "Media-Baja", "Muy Baja"]
mktsegment = ["Producto", "Distribución", "Reparto", "Precio", "Convalidado", "Jornada"]
flag = ["Verdadero", "Falso"]



def create_database(connection):
    connection.run("MATCH (n) DETACH DELETE n")
    connection.run("DROP INDEX A IF EXISTS")
    connection.run("DROP INDEX B IF EXISTS")
    connection.run("DROP INDEX C IF EXISTS")
    crear_nodo_partes(connection)
    crear_nodo_supp(connection)
    crear_nodo_partsupp(connection)
    crear_nodo_nacion(connection)
    crear_nodo_region(connection)
    crear_nodo_orders(connection)
    crear_nodo_customer(connection)
    crear_nodo_llineitem(connection)
    establecer_relaciones(connection)



def crear_nodo_partes(connection):
    for i in [0, 1, 2, 3, 4]:
        connection.run("CREATE (part" + key[i] + ": Part{p_partkey: " + key[i] + ", p_name: 'Partkey" + key[i] + "'"
                                                                                                                 ", p_mfgr: 'ABCDEFG', p_brand: '" +
                       brand[i] + "', p_type: 'Running'" +
                       ", p_size: " + str(random.randint(38, 45)) + ", p_container: 'Containter" + key[i] +
                       "', p_retailprice: " + str(float(random.randint(1000, 5000) / 100)) +
                       ", p_comment: 'OK'})")


def crear_nodo_supp(connection):
    for i in [0, 1, 2, 3, 4]:
        connection.run(
            "CREATE (supp" + key[i] + ": Supplier{s_suppkey: " + key[i] + ", s_name: 'Supplier" + key[i] +
            "', s_address: '" + address[i] + "', s_phone: " + str(random.randint(600000000, 699999999)) +
            ", s_acctbal: " + str(random.random()) + ", s_comment: 'OK'})")



def crear_nodo_partsupp(connection):
    for i in [0, 1, 2, 3, 4]:
        connection.run("CREATE (partsupp" + key[i] + ": PartSupp{ps_partkey: " + key[i] + ", ps_suppkey: " + key[i] +
                       ", ps_availqty: " + str(random.randint(100, 500)) +
                       ", ps_supplycost: " + str(float(random.randint(100, 500) / 100)) + ", ps_comment: 'OK'})")
    connection.run("CREATE INDEX A FOR (n:PartSupp) ON (n.ps_supplycost)")

def crear_nodo_nacion(connection):
    for i in [0, 1, 2, 3]:
        connection.run("CREATE (nation" + key[i] + ": Nation{n_nationkey: " + key[i] + ", n_name: '" + nation[i] + "'"
                                                                                                                   ", n_comment: 'OK'})")


def crear_nodo_region(connection):
    for i in [0, 1, 2]:
        connection.run("CREATE (region" + key[i] + ": Region{r_regionkey: " + key[i] + ", r_name: '" + region[i] + "'"
                                                                                                                   ", r_comment: 'OK'})")


def crear_nodo_orders(connection):
    for i in [0, 1, 2, 3, 4]:
        connection.run("CREATE (order" + key[i] + ": Order{o_orderkey: " + key[i] + ", o_orderstatus: 'OK" + "'"
                                                                                                             ", o_totalprice: " + str(
            random.randint(0, 1000)) + ", o_orderdate: '" + random.choice(date) +
                       "', o_orderpriority: '" + random.choice(priority) +
                       "', o_clerk: '" + "Louis" +
                       "', o_shippriority: '" + random.choice(priority) +
                       "', o_comment: 'OK'})")
    connection.run("CREATE INDEX B FOR (l:Order) ON (l.o_orderdate)")

def crear_nodo_customer(connection):
    for i in [0, 1, 2, 3, 4]:
        connection.run(
            "CREATE (customer" + key[i] + ": Customer{c_custkey: " + key[i] + ", c_name: 'Customer" + key[i] +
            "', c_address: '" + address[i] + "', c_phone: " + str(random.randint(600000000, 699999999)) +
            ", c_acctbal: " + str(random.random()) +
            ", c_mktsegment: '" + random.choice(mktsegment) +
            "', s_comment: 'OK'})")


def crear_nodo_llineitem(connection):
    for i in [0, 1, 2, 3, 4]:
        connection.run("CREATE (lineitem" + key[i] + ": Lineitem{l_linenumber: " + key[i] + ", l_quantity: " + str(
            random.randint(0, 100)) +
                       ", l_extendedprice: " + str(random.randint(0, 200)) + ", l_discount: " + str(
            random.randint(0, 99)) +
                       ", l_tax: " + str(random.randint(0, 20)) +
                       ", l_returnflag: '" + random.choice(flag) +
                       "', l_linestatus: '" + random.choice(flag) +
                       "', l_shipdate: '" + random.choice(date) +
                       "', l_commitdate: '" + random.choice(date) +
                       "', l_receiptdate: '" + random.choice(date) +
                       "', l_shipinstruct: 'Ok" + key[i] +
                       "', l_shipmode: 'Ok" + key[i] +
                       "', l_comment: 'OK'})")
    connection.run("CREATE INDEX C FOR (m:Lineitem) ON (m.l_shipdate)")

# RELATIONSHIPS BETWEEN NODES
def establecer_relaciones(connection):
    # PART --> PARTSUPP
    connection.run("MATCH (part1: Part{p_partkey: 1}), (partsupp1: PartSupp{ps_suppkey: 1, ps_partkey: 1}) "
                   "CREATE (part1) -[:BELONGS_TO]-> (partsupp1)")
    connection.run("MATCH (part2: Part{p_partkey: 2}), (partsupp2: PartSupp{ps_suppkey: 2, ps_partkey: 2}) "
                   "CREATE (part2) -[:BELONGS_TO]-> (partsupp2)")
    connection.run("MATCH (part3: Part{p_partkey: 3}), (partsupp3: PartSupp{ps_suppkey: 3, ps_partkey: 3}) "
                   "CREATE (part3) -[:BELONGS_TO]-> (partsupp3)")
    connection.run("MATCH (part4: Part{p_partkey: 4}), (partsupp4: PartSupp{ps_suppkey: 4, ps_partkey: 4}) "
                   "CREATE (part4) -[:BELONGS_TO]-> (partsupp4)")
    connection.run("MATCH (part5: Part{p_partkey: 5}), (partsupp5: PartSupp{ps_suppkey: 5, ps_partkey: 5}) "
                   "CREATE (part5) -[:BELONGS_TO]-> (partsupp5)")

    # PARTSUPP --> SUPPLIER
    connection.run("MATCH (partsupp1: PartSupp{ps_suppkey: 1, ps_partkey: 1}), (supp1: Supplier{s_suppkey: 1}) "
                   "CREATE (partsupp1) -[:BELONGS_TO]-> (supp1)")
    connection.run("MATCH (partsupp2: PartSupp{ps_suppkey: 2, ps_partkey: 2}), (supp2: Supplier{s_suppkey: 2}) "
                   "CREATE (partsupp2) -[:BELONGS_TO]-> (supp2)")
    connection.run("MATCH (partsupp3: PartSupp{ps_suppkey: 3, ps_partkey: 3}), (supp3: Supplier{s_suppkey: 3}) "
                   "CREATE (partsupp3) -[:BELONGS_TO]-> (supp3)")
    connection.run("MATCH (partsupp4: PartSupp{ps_suppkey: 4, ps_partkey: 4}), (supp4: Supplier{s_suppkey: 4}) "
                   "CREATE (partsupp4) -[:BELONGS_TO]-> (supp4)")
    connection.run("MATCH (partsupp5: PartSupp{ps_suppkey: 5, ps_partkey: 5}), (supp5: Supplier{s_suppkey: 5}) "
                   "CREATE (partsupp5) -[:BELONGS_TO]-> (supp5)")

    # SUPPLIER --> NATION
    connection.run("MATCH (supp1: Supplier{s_suppkey: 1}), (nation1: Nation{n_nationkey: 1}) "
                   "CREATE (supp1) -[:BELONGS_TO]-> (nation1)")
    connection.run("MATCH (supp2: Supplier{s_suppkey: 2}), (nation1: Nation{n_nationkey: 1}) "
                   "CREATE (supp2) -[:BELONGS_TO]-> (nation1)")
    connection.run("MATCH (supp3: Supplier{s_suppkey: 3}), (nation2: Nation{n_nationkey: 2}) "
                   "CREATE (supp3) -[:BELONGS_TO]-> (nation2)")
    connection.run("MATCH (supp4: Supplier{s_suppkey: 4}), (nation3: Nation{n_nationkey: 3}) "
                   "CREATE (supp4) -[:BELONGS_TO]-> (nation3)")
    connection.run("MATCH (supp5: Supplier{s_suppkey: 5}), (nation4: Nation{n_nationkey: 4}) "
                   "CREATE (supp5) -[:BELONGS_TO]-> (nation4)")

    # NATION --> REGION
    connection.run("MATCH (nation1: Nation{n_nationkey: 1}), (region1: Region{r_regionkey: 1}) "
                   "CREATE (nation1) -[:BELONGS_TO]-> (region1)")
    connection.run("MATCH (nation2: Nation{n_nationkey: 2}), (region1: Region{r_regionkey: 1}) "
                   "CREATE (nation2) -[:BELONGS_TO]-> (region1)")
    connection.run("MATCH (nation3: Nation{n_nationkey: 3}), (region2: Region{r_regionkey: 2}) "
                   "CREATE (nation3) -[:BELONGS_TO]-> (region2)")
    connection.run("MATCH (nation4: Nation{n_nationkey: 4}), (region3: Region{r_regionkey: 3}) "
                   "CREATE (nation4) -[:BELONGS_TO]-> (region3)")

    # CUSTOMER --> NATION
    connection.run("MATCH (customer1: Customer{c_custkey: 1}), (nation1: Nation{n_nationkey: 1}) "
                   "CREATE (customer1) -[:BELONGS_TO]-> (nation1)")
    connection.run("MATCH (customer2: Customer{c_custkey: 2}), (nation2: Nation{n_nationkey: 2}) "
                   "CREATE (customer2) -[:BELONGS_TO]-> (nation2)")
    connection.run("MATCH (customer3: Customer{c_custkey: 3}), (nation3: Nation{n_nationkey: 3}) "
                   "CREATE (customer3) -[:BELONGS_TO]-> (nation3)")
    connection.run("MATCH (customer4: Customer{c_custkey: 4}), (nation4: Nation{n_nationkey: 4}) "
                   "CREATE (customer4) -[:BELONGS_TO]-> (nation4)")
    connection.run("MATCH (customer5: Customer{c_custkey: 5}), (nation1: Nation{n_nationkey: 1}) "
                   "CREATE (customer5) -[:BELONGS_TO]-> (nation1)")

    # CUSTOMER --> ORDER
    connection.run("MATCH (customer1: Customer{c_custkey: 1}), (order1: Order{o_orderkey: 1}) "
                   "CREATE (customer1) -[:BELONGS_TO]-> (order1)")
    connection.run("MATCH (customer2: Customer{c_custkey: 2}), (order2: Order{o_orderkey: 2}) "
                   "CREATE (customer2) -[:BELONGS_TO]-> (order2)")
    connection.run("MATCH (customer3: Customer{c_custkey: 3}), (order3: Order{o_orderkey: 3}) "
                   "CREATE (customer3) -[:BELONGS_TO]-> (order3)")
    connection.run("MATCH (customer4: Customer{c_custkey: 4}), (order4: Order{o_orderkey: 4}) "
                   "CREATE (customer4) -[:BELONGS_TO]-> (order4)")
    connection.run("MATCH (customer5: Customer{c_custkey: 5}), (order5: Order{o_orderkey: 5}) "
                   "CREATE (customer5) -[:BELONGS_TO]-> (order5)")

    # ORDER --> LINEITEM
    connection.run("MATCH (order1: Order{o_orderkey: 1}), (lineitem1: Lineitem{l_linenumber: 1}) "
                   "CREATE (order1) -[:BELONGS_TO]-> (lineitem1)")
    connection.run("MATCH (order2: Order{o_orderkey: 2}), (lineitem2: Lineitem{l_linenumber: 2}) "
                   "CREATE (order2) -[:BELONGS_TO]-> (lineitem2)")
    connection.run("MATCH (order3: Order{o_orderkey: 3}), (lineitem3: Lineitem{l_linenumber: 3}) "
                   "CREATE (order3) -[:BELONGS_TO]-> (lineitem3)")
    connection.run("MATCH (order4: Order{o_orderkey: 4}), (lineitem4: Lineitem{l_linenumber: 4}) "
                   "CREATE (order4) -[:BELONGS_TO]-> (lineitem4)")
    connection.run("MATCH (order5: Order{o_orderkey: 5}), (lineitem5: Lineitem{l_linenumber: 5}) "
                   "CREATE (order5) -[:BELONGS_TO]-> (lineitem5)")

    # LINEITEM --> PARTSUPP
    connection.run("MATCH (lineitem1: Lineitem{l_linenumber: 1}), (partsupp1: PartSupp{ps_suppkey: 1, ps_partkey: 1}) "
                   "CREATE (lineitem1) -[:BELONGS_TO]-> (partsupp1)")
    connection.run("MATCH (lineitem2: Lineitem{l_linenumber: 2}), (partsupp2: PartSupp{ps_suppkey: 2, ps_partkey: 2}) "
                   "CREATE (lineitem2) -[:BELONGS_TO]-> (partsupp2)")
    connection.run("MATCH (lineitem3: Lineitem{l_linenumber: 3}), (partsupp3: PartSupp{ps_suppkey: 3, ps_partkey: 3}) "
                   "CREATE (lineitem3) -[:BELONGS_TO]-> (partsupp3)")
    connection.run("MATCH (lineitem4: Lineitem{l_linenumber: 4}), (partsupp4: PartSupp{ps_suppkey: 4, ps_partkey: 4}) "
                   "CREATE (lineitem4) -[:BELONGS_TO]-> (partsupp4)")
    connection.run("MATCH (lineitem5: Lineitem{l_linenumber: 5}), (partsupp5: PartSupp{ps_suppkey: 5, ps_partkey: 5}) "
                   "CREATE (lineitem5) -[:BELONGS_TO]-> (partsupp5)")


# QUERIES
def query1(connection, date):
    return \
        connection.run("MATCH (li: Lineitem) "
                       "WHERE li.l_shipdate <= $date "
                       "RETURN li.l_returnflag AS l_returnflag, li.l_linestatus AS l_linestatus, sum(li.l_quantity) AS sum_qty, "
                       "sum(li.l_extendedprice) AS sum_base_price, sum(li.l_extendedprice * (1 - li.l_discount)) AS sum_disc_price, "
                       "sum(li.l_extendedprice * (1 - li.l_discount) * (1 + li.l_tax)) AS sum_charge, avg(li.l_quantity) AS avg_qty, "
                       "avg(li.l_extendedprice) AS avg_price, AVG(li.l_discount) AS avg_disc, COUNT(*) AS count_order "
                       "ORDER BY li.l_returnflag, li.l_linestatus",
                       {"date": date})


def query2(connection, size, type, region):
    min_supplycost = 1.0
    for row in sub_query2(connection, region):
        min_supplycost = float(row["min_supplycost"])

    return \
        connection.run("MATCH (p: Part)-[:BELONGS_TO]->(ps: PartSupp)-[:BELONGS_TO]->(s: Supplier)-[:BELONGS_TO]->"
                       "(n: Nation)-[:BELONGS_TO]->(r: Region) "
                       "WHERE p.p_size = $size and p.p_type = $type and r.r_name = $region and ps.ps_supplycost = $min_supplycost "
                       "RETURN s.s_acctbal AS s_acctbal, s.s_name AS s_name, n.n_name AS n_name, p.p_partkey AS p_partkey, "
                       "p.p_mfgr AS p_mfgr, s.s_address AS s_address, s.s_phone AS s_phone, s.s_comment AS s_comment "
                       "ORDER BY s.s_acctbal DESC, n.n_name, s.s_name, p.p_partkey",
                       {"size": size,
                        "type": type,
                        "region": region,
                        "min_supplycost": min_supplycost})


def sub_query2(connection, region):
    return \
        connection.run("MATCH "
                       "(ps: PartSupp)-[:BELONGS_TO]->(s: Supplier)-[:BELONGS_TO]->(n: Nation)-[:BELONGS_TO]->(r: Region) "
                       "WHERE r.r_name = $region "
                       "RETURN min(ps.ps_supplycost) AS min_supplycost",
                       {"region": region})


def query3(connection, mkt_segment, date1, date2):
    return \
        connection.run("MATCH (c: Customer)-[:BELONGS_TO]->(o: Order)-[:BELONGS_TO]->(li: Lineitem) "
                       "WHERE li.l_shipdate > $date2 and o.o_orderdate < $date1 and c.c_mktsegment = $mkt_segment "
                       "RETURN o.o_orderkey AS l_orderkey, o.o_orderdate AS o_orderdate, o.o_shippriority AS o_shippriority, "
                       "sum(li.l_extendedprice * (1 - li.l_discount)) AS revenue "
                       "ORDER BY revenue DESC, o.o_orderdate",
                       {"date1": date1,
                        "date2": date2,
                        "mkt_segment": mkt_segment})


def query4(connection, date1, date2, region):
    return \
        connection.run("MATCH (c: Customer)-[:BELONGS_TO]->(o: Order)-[:BELONGS_TO]->(li: Lineitem)-[:BELONGS_TO]->"
                       "(ps: PartSupp)-[:BELONGS_TO]->(s: Supplier)-[:BELONGS_TO]->(n: Nation)-[:BELONGS_TO]->(r: Region) "
                       "WHERE o.o_orderdate >= $date1 and o.o_orderdate < $date2 and r.r_name = $region "
                       "RETURN n.n_name AS n_name, sum(li.l_extendedprice * (1 - li.l_discount)) AS revenue "
                       "ORDER BY revenue DESC",
                       {"date1": date1,
                        "date2": date2,
                        "region": region})


# AUX
def valid_date(date):
    year, month, day = date.split('-')
    try:
        dt.datetime(int(year), int(month), int(day))
    except ValueError:
        return False
    return True


# MAIN
def main():
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "cbde"))
    connection = driver.session()
    create_database(connection)

    print("Escoge la acción que deseas realizar\n",
          "1: query 1\n",
          "2: query 2\n",
          "3: query 3\n",
          "4: query 4\n",
          "0: Mostrar base de datos actual\n",
          "-1: Exit")

    op = input("Introduce la opción deseada\n")
    op = int(op)

    while op != -1:
        if op == 0:
            for item in connection.run('MATCH (n)-[r]->(m) RETURN n, r, m'):
                print(item)

        elif op == 1:
            date_param = input("Introduce una shipdate de lineitem en formato YYYY-mm-dd: ")
            while not valid_date(date_param):
                date_param = input("Introduce una shipdate de lineitem en formato YYYY-mm-dd: ")

            q1 = query1(connection,
                        str(dt.datetime.strptime(date_param, "%Y-%m-%d")))

            print("Resultados query 1:")
            for row in q1:
                print(row)

        elif op == 2:
            size_param = input("Introduce un size de part: ")
            while not size_param.isdigit():
                size_param = input("Introduce un size de part: ")

            type_param = input("Introduce un type de part: ")

            region_param = input("Introduce un name de region: ")

            q2 = query2(connection,
                        int(size_param),
                        str(type_param),
                        str(region_param))

            print("Resultados query 2:")
            for row in q2:
                print(row)

        elif op == 3:
            mkt_segment_param = input("Introduce un mkt_segment de un customer: ")

            date1_param = input("Introduce una orderdate de un order en formato YYYY-mm-dd: ")
            while not valid_date(date1_param):
                date1_param = input("Introduce una orderdate de un order en formato YYYY-mm-dd: ")

            date2_param = input("Introduce una shipdate de un lineitem en formato YYYY-mm-dd: ")
            while not valid_date(date2_param):
                date2_param = input("Introduce una shipdate de un lineitem en formato YYYY-mm-dd: ")

            q3 = query3(connection,
                        str(mkt_segment_param),
                        str(dt.datetime.strptime(date1_param, "%Y-%m-%d")),
                        str(dt.datetime.strptime(date2_param, "%Y-%m-%d")))

            print("Resultados query 3:")
            for row in q3:
                print(row)

        elif op == 4:
            date = input("Introduce una orderdate de un order en formato YYYY-mm-dd: ")
            while not valid_date(date):
                date = input("Introduce una orderdate de un order en formato YYYY-mm-dd: ")

            date_param = dt.datetime.strptime(date, "%Y-%m-%d")
            date2_param = date_param.replace(date_param.year + 1)

            region_param = input("Introduce un name de una region: ")

            q4 = query4(connection,
                        str(date_param),
                        str(date2_param),
                        str(region_param))

            print("Resultados query 4:")
            for row in q4:
                print(row)

        elif op == -1:
            break

        print("Escoge la acción que deseas realizar\n",
              "1: query 1\n",
              "2: query 2\n",
              "3: query 3\n",
              "4: query 4\n",
              "0: Mostrar base de datos actual\n",
              "-1: Exit")

        op = input("Introduce la opción deseada\n")
        op = int(op)


if __name__ == "__main__":
    main()
