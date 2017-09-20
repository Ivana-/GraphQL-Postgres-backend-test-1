(ns project-lein-1.core
  (:require [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [honeysql.helpers :refer :all :as helpers]
            )
  (:gen-class))

;------------------------- параметры подключения к базе данных postgres

; jdbc:postgresql://localhost:5432/contactdb
(def db-spec
  {:dbtype "postgresql"
   :host "localhost"
   :dbname "contactdb"
   :user "postgres"
   :password "postgress"})

;--------------- параметры соответствия полей graphql запроса базе данных postgres

; соответствия имен полей
(def tables-map {
                 :persons {:__table :JC_CONTACT,
                           :key :contact_id,
                           :id :contact_id,
                           :name :first_name
                           :surname :last_name},
                 :posts {:__table :POSTS,
                         :key :post_id,
                         :id :post_id,
                         :text :title},
                 :comments {:__table :COMMS,
                            :key :comm_id,
                            :id :comm_id,
                            :text :comm_text},
                 :childrens {:__table :CHILDRENS,
                             :key :children_id,
                             :id :children_id,
                             :name :first_name}
                 })

; соответствия условий
(def join-conditions-map
  {:persons {:posts     [:= :JC_CONTACT.contact_id :POSTS.contact_id]
             :childrens [:= :JC_CONTACT.contact_id :CHILDRENS.contact_id]
             :comments  [:= :COMMS.contact_id :JC_CONTACT.contact_id]

             ;:comments  [:and [:= :POSTS.post_id :COMMS.post_id]
             ;           [:= :COMMS.contact_id :JC_CONTACT.contact_id]]
             }
   :posts   {:comments  [:= :POSTS.post_id :COMMS.post_id]}
   :comments  {:persons [:= :COMMS.contact_id :JC_CONTACT.contact_id]
               :posts   [:= :POSTS.post_id :COMMS.post_id]}
   })

;--------------- служебные функции преобразования имен

(defn get-gql-name [node] (:key node))
(defn get-sql-table-key [node] (:key ((get-gql-name node) tables-map)))
(defn get-sql-table-name [node] (:__table ((get-gql-name node) tables-map)))

;--------------- функции построения иерархии graphql-запроса

; строит дерево по ререданным аргументам. примеры в мэйне
(defn make-q-tree [k fs & nodes] {:key k, :fields fs, :value nil, :nodes nodes})

; назначает тип данных "дерево иерархии" экземпляром класса Функтор :)
(defn map-tree [f node] (f (assoc node :nodes (map f (:nodes node)))))

; функторное преобразование всех нод дерева - меняем ключ, добавляем имя graphql
(defn make-template-tree [node]
  (map-tree #(assoc %1 :key (get-sql-table-key %1) :gql-name (:key %1)) node))

;--------- функции построения sql-запроса по заданной иерархии graphql-запроса

; условия джойнов таблиц
(defn get-join-conditions [node parent-node]
  (if (nil? parent-node)
    []
    (let [gql-parent-name (get-gql-name parent-node)
          gql-name (get-gql-name node)]
      ;(println (gql-name (gql-parent-name join-conditions-map))
      (gql-name (gql-parent-name join-conditions-map)) )))

; добавления поля в блок SELECT
(defn add-field-to-sql-map [m field node parent-node]
  (let [sql-field (field ((get-gql-name node) tables-map))
        sql-table (get-sql-table-name node)
        sql-field-name (keyword (str (name sql-table) "." (name sql-field)))
        ;sql-field-pseudo (str (name sql-table) "_" (name sql-field))
        ]
    ;[:b.bla "bla-bla"]
    ;(merge-select m [sql-field-name sql-field-pseudo])
    (merge-select m sql-field-name)
    ))

; добавление ноды в sql-запрос
(defn add-node-to-sql-map [m node parent-node]
  (let [m-new (reduce #(add-field-to-sql-map %1 %2 node parent-node) m (:fields node))
        sql-table (get-sql-table-name node)
        join-conditions (get-join-conditions node parent-node)]
    (if (nil? parent-node)
      (from m-new sql-table)
      (merge-left-join m-new sql-table join-conditions))
    ))

; рекурсивная функция - ядро построения sql-запроса по дереву иерархии
(defn make-sql-map-core [m node parent-node]
  (let [m-new (reduce #(make-sql-map-core %1 %2 node) m (:nodes node))]
    (add-node-to-sql-map m-new node parent-node)))

; основная функция - строит текст запроса по иерархии graphql-запроса
(defn make-sql-map [node] (make-sql-map-core {} node nil))

; -------------------  функции алгоритма постобработки линейной таблицы запроса
; -------------------  для построения иерархической структуры

; создание объекта из строки результата запроса (список полей в ноде дерева)
(defn make-obj [node r]
  (let [gql-sql-map ((:gql-name node) tables-map)]
    (reduce (fn [a x] (assoc a x ( (x gql-sql-map) r))) {} (:fields node))))

; значение текущей ноды изменилось в данной строке результата запроса
(defn is-changed [node r]
  (let [k (:key node) val-list (:value node)]
    (and (not (nil? (k r))) (or (nil? val-list) (not= (:id (first val-list)) (k r))))))

; добавляет объект в ноду (на вершину списка :value)
(defn update-node [node r]
  (let [val-list (:value node)]
    (assoc node :value (cons (make-obj node r) val-list))))

; добавляет накопленные вложенные сложные поля к верхнему (актуальному) объекту ноды
(defn add-coll-val [node coll-ns]
  (let [o (first (:value node)) t (rest (:value node))]
    (cons (merge o (zipmap (map :key coll-ns) (map :value coll-ns))) t)
    ))

; очистка значений всех подчиненных нод
(defn deep-clear-node [node]
  (assoc node :value nil :nodes (map deep-clear-node (:nodes node))))

; "сборка" подчиненных полей ноды в объект и помещение его в поле :value
(defn deep-collect-node [node]
  (if (or (nil? (:nodes node)) (nil? (:value node))) node
  (let [coll-ns (map deep-collect-node (:nodes node))]
    ;(dissoc (assoc node :value (add-coll-val node coll-ns)) :nodes)
    (assoc node :value (add-coll-val node coll-ns))
    )))

; свертка ноды - сборка объекта и очистка значений дочерних нод
(defn collaps-node [node]
  (let [collected-node (deep-collect-node node)
        clear-node (deep-clear-node node)]
    (assoc node :nodes (:nodes clear-node) :value (:value collected-node))
    ))

; подготовка дерева - свернем все ноды самых верхних уровней, которые изменились
; в текущей строке результата запроса
(defn prepare-tree [node r]
  (if (is-changed node r) (collaps-node node)
                          (assoc node :nodes (map #(prepare-tree %1 r) (:nodes node))) ))

; добавим новые значения во все ноды, которые изменились (или были очищены)
(defn update-tree [node r]
  (let [upd-node (if (is-changed node r) (update-node node r) node)]
    (assoc upd-node :nodes (map #(update-tree %1 r) (:nodes upd-node)))
    ))

; функция - ядро свертки линейной последовательности строк запроса
(defn reduce-query-core [a r] (update-tree (prepare-tree a r) r))

; основная функция - свертка результата запроса с накоплением результата
(defn query-from-gql-tree-to-hierarchy [gql-tree rows]
  (doseq [r rows]
    (println ">" r))

  (let [z0 (reduce reduce-query-core gql-tree rows)
        z (collaps-node z0)
        ;v (:value z)
        ]
    z
    ))

; --------------------- функции красивой иерархической печати структуры

(def new-level-wide "    ")

; рекурсивная функция - ядро иерархической печати объекта
(defn my-print-object-core [o d]
  (cond
    (map? o) (do
               (println (str d "{"))
               (doseq [k (keys o)]
                 (print (str new-level-wide d k " "))
                 (my-print-object-core (k o) (str new-level-wide d)))
               (println (str d "}")))
    (seq? o) (do
               (println)
               (println (str d "("))
               (doseq [e o]
                 (my-print-object-core e (str new-level-wide d)))
               (println (str d ")"))
               )
    :else (println o)))

; основная функция красивой печати
(defn my-print-object [o]
  (println "------------------------------------------")
  (println o)
  (my-print-object-core o ""))


; -------------------------  собственно мэйн

(defn -main [& args]
  (let [; различные варианты ast из graphql-запросов (пока вручную)
        ; работают не все варианты
        querry-tree-1
        (make-q-tree :persons [:id, :name, :surname]
                     (make-q-tree :posts [:id, :text]
                                  ;(make-q-tree :comments [:id, :text])
                                  )
                     ;(make-q-tree :childrens [:id, :name])
                     ;(make-q-tree :comments [:id, :text])
                     )

        querry-tree-2
        (make-q-tree :posts [:id, :text]
                     (make-q-tree :comments [:id, :text]))

        querry-tree-3
        (make-q-tree :posts [:id, :text]
                     (make-q-tree :comments [:id, :text]
                                  (make-q-tree :persons [:id, :name, :surname])))
        querry-tree-4
        (make-q-tree :comments [:id, :text]
                     (make-q-tree :persons [:id, :name, :surname])
                     )
        querry-tree-5
        (make-q-tree :comments [:id, :text]
                     (make-q-tree :posts [:id, :text]))

        ; выбранный вариант запроса
        querry-tree querry-tree-1
        sql-query (sql/format (make-sql-map querry-tree))
        start-tree (make-template-tree querry-tree)]

    (println sql-query)
    ;(println querry-tree)
    ;(println start-tree)
    (my-print-object
      (query-from-gql-tree-to-hierarchy start-tree (jdbc/query db-spec sql-query)))
    ))
