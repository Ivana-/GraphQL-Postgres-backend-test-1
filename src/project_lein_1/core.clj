(ns project-lein-1.core

  (:require [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [honeysql.helpers :refer :all :as helpers]
            )
  ;(require '[clojure.java.jdbc :as j])
  ;(:require [clojure.java.jdbc :as j])

  (:gen-class))

(def options (to-array ["Yes, please" "No, thanks" "No eggs, no ham!"]))
(def possibilities (to-array ["ham" "spam" "yam"]))

(defn fact [x] (if (> x 1) (* x (fact (- x 1))) 1))
;(defn fact1 [x a] (if (> x 1) (fact1 (- x 1) (* x a)) a))
(defn fact1 [x a] (if (> x 1) (fact1 (- x 1) (* x a)) a))

; jdbc:postgresql://localhost:5432/contactdb

(def db-spec
  {:dbtype "postgresql"
   :host "localhost"
   :dbname "contactdb"
   :user "postgres"
   :password "postgress"})

;(def reduce-start-value
;  {:contact_id nil,
;   :post_id nil,
;   :comm_id nil}
;  )

(defn make-tree [k v & nodes] {:key k, :value v, :nodes nodes})

(def reduce-start-value-tree
  (make-tree :contact_id nil
             (make-tree :post_id nil (make-tree :comm_id nil (make-tree :author_id nil)))
             (make-tree :children_id nil)
             ))

(defn make-obj--- [k r]
  (case k
    :contact_id   {:id (:contact_id r), :name (:first_name r)}
    :post_id      {:id (:post_id r), :name (:title r)}
    :comm_id      {:id (:comm_id r), :name (:comm_text r)}
    :children_id  {:id (:children_id r), :name (:children_name r)}
    :author_id    {:id (:author_id r), :name (:author_name r)}
    "error make-obj"))

(declare tables-map)

(defn make-obj [node r]
  (let [gql-sql-map ((:gql-name node) tables-map)]
    (reduce (fn [a x] (assoc a x ( (x gql-sql-map) r))) {} (:fields node))))



(defn is-changed [node r]
  (let [k (:key node) val-list (:value node)]
    (and (not (nil? (k r))) (or (nil? val-list) (not= (:id (first val-list)) (k r))))))

(defn update-node [node r]
  (let [val-list (:value node)]
    (assoc node :value (cons (make-obj node r) val-list))))

;(defn add-coll-val-t1 [node coll-ns]
;  (let [o (first (:value node)) t (rest (:value node))]
;    (cons (assoc o :chs coll-ns) t)
;    ))

(defn add-coll-val [node coll-ns]
  (let [o (first (:value node)) t (rest (:value node))]
    (cons (merge o (zipmap (map :key coll-ns) (map :value coll-ns))) t)
    ))


(defn deep-clear-node [node]
  (assoc node :value nil :nodes (map deep-clear-node (:nodes node))))

;(defn collaps-node [node]
;  (let [coll-ns (map collaps-node (:nodes node))
;        clear-node (deep-clear-node node)]
;    (assoc node :nodes (:nodes clear-node) :value (add-coll-val node coll-ns))
;    ))

(defn deep-collect-node [node]
  (if (or (nil? (:nodes node)) (nil? (:value node))) node
  (let [coll-ns (map deep-collect-node (:nodes node))]
    ;(assoc node :value (add-coll-val node coll-ns) :nodes nil)
    ;(dissoc (assoc node :value (add-coll-val node coll-ns)) :nodes)
    (assoc node :value (add-coll-val node coll-ns))
    )))

;(defn collaps-node [node]
;  (let [collected-node (deep-collect-node node)
;        clear-node (deep-clear-node node)]
;    (assoc node :nodes (:nodes clear-node) :value (:value collected-node))
;    ))

(defn deep-collect-node-t1 [node]
  (let [ns (:nodes node) o (first (:value node)) t (rest (:value node))]
    ;(if (nil? ns) {}
    ;              (let [coll-ns (map deep-collect-node ns)]
    ;(println (class (keys ns)) " " (class coll-ns))
    ;(println (class (zipmap (keys ns) coll-ns)))

    ;(println "nn" node)
    ;(println "zz" (zipmap (map :key ns) coll-ns))
    ;                (zipmap (map :key ns) (map :value ns))
    ;                )
    ;              ))
    (let [coll-ns (map deep-collect-node ns)]

;    {(:key node) (cons (merge o (zipmap (map :key ns) (map :value ns))) t)}

    {(:key node) (cons (merge o (zipmap (map :key ns) (map :value coll-ns))) t)}
      )
    )
  ;{:o 33}
  )

;(defn upd-cur-node-val [node o-map]
;  (let [o (first (:value node)) t (rest (:value node))]
;    (cons (merge o o-map) t)
;    ))

(defn collaps-node [node]
  (let [collected-node (deep-collect-node node)
        clear-node (deep-clear-node node)]
    (assoc node :nodes (:nodes clear-node) :value (:value collected-node))
    ))

(defn prepare-tree [node r]
  (if (is-changed node r) (collaps-node node)
                          (assoc node :nodes (map #(prepare-tree %1 r) (:nodes node))) ))

(defn update-tree [node r]
  (let [upd-node (if (is-changed node r) (update-node node r) node)]
    (assoc upd-node :nodes (map #(update-tree %1 r) (:nodes upd-node)))
    ))

;(defn add-val-by-key [k a r]
;  (assoc a k (cons (k r) (k a))))

;(defn add-if-changed [k a r]
;  (if (or (nil? (k a)) (not= (first (k a)) (k r))) (add-val-by-key k a r) a))

(defn reduce-query-core [a r] (update-tree (prepare-tree a r) r))


; ----------------------------------- pretty print ----------------------------------
(def new-level-wide "    ")

(comment
(declare print-tree-core)

(defn print-value-core [v d]
  ;(println d "value")
  (doseq [e v]
    ;(println d e)
    (println d (:id e) (:name e))
    ;(doseq [ch (:chs e)]
    ;  (println (str new-level-wide d) ":chs ----")
    ;  (print-tree-core ch (str new-level-wide d))
    ;  )
    )
  )

(defn print-tree-core [node d]
  (println d (:key node))
  (print-value-core (:value node) d)
  (doseq [n (:nodes node)]
    (print-tree-core n (str new-level-wide d)))
  )
(defn print-tree [node]
  (println "------------------------------------------")
  (println node)
  (print-tree-core node ""))
)

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

(defn my-print-object [o]
  (println "------------------------------------------")
  (println o)
  (my-print-object-core o ""))

; -----------------------------------          ----------------------------------
(defn query-to-hierarchy [rows]
  ;(println (class rows))
  ;(println (seq? rows))
;  (doseq [r rows]
;    (println ">" r))
  ;(doseq [r rows]
  ;  (println "=" (class r) r))
  (let [z0 (reduce reduce-query-core reduce-start-value-tree rows)
        z (collaps-node z0)
        ;z z0
        ]
    ;(println z)
    ;(println "Error on collapsing!")
    ;(println (collaps-node z))
    ;(println reduce-start-value-tree)
    ;(println (deep-clear-node (collaps-node z)))

    (my-print-object z)
    "---------------------------"

    ;(print-tree (collaps-node z))

    ;println (deep-collect-node z))
    ))








;(def tables-map {
;  :persons {:__table "JC_CONTACT", :key :contact_id, :id "CONTACT_ID", :name "FIRST_NAME"},
;  :posts {:__table "POSTS", :key :post_id, :id "POST_ID", :text "TITLE"},
;  :childrens {:__table "CHILDRENS", :key :children_id,  :id "CHILDREN_ID", :text "FIRST_NAME"}
;                 })
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

(defn get-gql-name [node] (:key node))
(defn get-sql-table-key [node] (:key ((get-gql-name node) tables-map)))
(defn get-sql-table-name [node] (:__table ((get-gql-name node) tables-map)))

(defn get-join-conditions [node parent-node]
  (if (nil? parent-node)
    []
    (let [gql-parent-name (get-gql-name parent-node)
          gql-name (get-gql-name node)]
      ;(println (gql-name (gql-parent-name join-conditions-map))
      (gql-name (gql-parent-name join-conditions-map)) )))

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

(defn add-node-to-sql-map [m node parent-node]
  (let [m-new (reduce #(add-field-to-sql-map %1 %2 node parent-node) m (:fields node))
        sql-table (get-sql-table-name node)
        join-conditions (get-join-conditions node parent-node)]
    (if (nil? parent-node)
      (from m-new sql-table)
      (merge-left-join m-new sql-table join-conditions))
    ))

(defn make-sql-map-core [m node parent-node]
  (let [m-new (reduce #(make-sql-map-core %1 %2 node) m (:nodes node))]
    (add-node-to-sql-map m-new node parent-node)))

(defn make-sql-map [node] (make-sql-map-core {} node nil))





(defn query-from-gql-tree-to-hierarchy [gql-tree rows]
  (doseq [r rows]
    (println ">" r))

  (let [z0 (reduce reduce-query-core gql-tree rows)
        z (collaps-node z0)
        v (:value z)
        ]
    (my-print-object v)))

(defn make-q-tree [k fs & nodes] {:key k, :fields fs, :value nil, :nodes nodes})

(defn map-tree [f node] (f (assoc node :nodes (map f (:nodes node)))))

(comment querry-tree
  (make-q-tree :persons [:id, :name, :surname]
               (make-q-tree :posts [:id, :text]
                            ;(make-q-tree :comments [:id, :text])
                            )
               ;(make-q-tree :childrens [:id, :name])
               ;(make-q-tree :comments [:id, :text])
               ))
(comment querry-tree (make-q-tree :posts [:id, :text] (make-q-tree :comments [:id, :text])))
(comment querry-tree
  (make-q-tree :posts [:id, :text]
               (make-q-tree :comments [:id, :text]
                            (make-q-tree :persons [:id, :name, :surname]))
               ))
(comment querry-tree
         (make-q-tree :comments [:id, :text]
                      (make-q-tree :persons [:id, :name, :surname])))
(def querry-tree
         (make-q-tree :comments [:id, :text]
                      (make-q-tree :posts [:id, :text])))


(defn make-template-tree [node]
  (map-tree #(assoc %1 :key (get-sql-table-key %1) :gql-name (:key %1)) node))

(defn -main [& args]
  ;(println querry-tree)
  ;(println (map-tree #(assoc %1 :value 33) querry-tree))
  ;(println (map-tree #(dissoc %1 :value) querry-tree))

  ;(println (sql/format (make-sql-map querry-tree)))
  (let [q (sql/format (make-sql-map querry-tree))
        start-tree (make-template-tree querry-tree)]
    (println q)
    (println querry-tree)
    (println start-tree)
    (println (query-from-gql-tree-to-hierarchy start-tree (jdbc/query db-spec q)))
    )
  )

(defn ----main
  "I don't do a whole lot ... yet."
  [& args]

  ;(println reduce-start-value-tree)
  ;(println (fact 6))
  ;(println (fact1 6 1))
  ;(println (jdbc/query db-spec ["SELECT 3*5 AS result"]))

  ;; (def rs (jdbc/query db-spec ["SELECT first_name, last_name FROM JC_CONTACT"]) )
  ;; (println rs)

  (def query-string "
    SELECT
    JC_CONTACT.CONTACT_ID,
    JC_CONTACT.FIRST_NAME,
    POSTS.POST_ID,
    POSTS.TITLE,
    COMMS.COMM_ID,
    COMMS.COMM_TEXT,
    CHILDRENS.CHILDREN_ID,
    CHILDRENS.FIRST_NAME AS CHILDREN_NAME,
    COMM_AUTHORS.CONTACT_ID AS AUTHOR_ID,
    COMM_AUTHORS.FIRST_NAME AS AUTHOR_NAME
    FROM JC_CONTACT
    LEFT OUTER JOIN POSTS ON (JC_CONTACT.contact_id = POSTS.contact_id)
    LEFT OUTER JOIN COMMS ON (POSTS.post_id = COMMS.post_id)
    LEFT OUTER JOIN CHILDRENS ON (JC_CONTACT.contact_id = CHILDRENS.contact_id)
    LEFT OUTER JOIN JC_CONTACT AS COMM_AUTHORS ON (COMM_AUTHORS.contact_id = COMMS.contact_id)
    ")

  (if (= 0 0)
    (let [q
          ;["SELECT * FROM JC_CONTACT"]
          ;["SELECT contact_id, first_name, last_name FROM JC_CONTACT WHERE contact_id = 1"]
          ;["SELECT contact_id, first_name, last_name FROM JC_CONTACT WHERE contact_id = ?" 1]
          ;["SELECT first_name, last_name FROM JC_CONTACT WHERE contact_id = ?" 1]
          [query-string]]
      (println "raw:")
      (println (query-to-hierarchy (jdbc/query db-spec q)))
      )
    ;; (def sqlmap {:select [:a :b :c]
    ;;              :from [:foo]
    ;;              :where [:= :f.a "baz"]})
    (do
      (def sqlmap-base {:select [:first_name :last_name]
                         :from   [:JC_CONTACT]
                         :where  [:= :contact_id 1]
                         }
                        ; sqlmap (sql/build sqlmap-base :where [:= :contact_id 2])
                        ; sqlmap (-> sqlmap-base (:where [:= :contact_id 2]))
                        )
      ; (def sqlmap (sql/build sqlmap-base :where [:= :contact_id 2]))
      ; (def sqlmap (-> sqlmap-base (select :*)))
      ; (def sqlmap (-> sqlmap-base (where [:= :contact_id 2]) sql/format))

      (def sqlmap
        (-> (select :*)
            (from :foo)
            (where [:in :foo.a (-> (select :a) (from :bar))])
            sql/format)
        )
      (println "honeysql:")
      (println (identity sqlmap))
      ; (println (jdbc/query db-spec (identity sqlmap)))
      )
    )

  ;  (javax.swing.JOptionPane/showMessageDialog nil "Hello World")
  ;  (javax.swing.JOptionPane/showMessageDialog nil "Hello World" "Inane warning" javax.swing.JOptionPane/WARNING_MESSAGE)
  ;  (javax.swing.JOptionPane/showOptionDialog
  ;    nil
  ;    "Would you like some green eggs to go with that ham?"
  ;    "A Silly Question"
  ;    javax.swing.JOptionPane/YES_NO_CANCEL_OPTION
  ;    javax.swing.JOptionPane/QUESTION_MESSAGE
  ;    nil
  ;    options ;(vec ["Yes, please" "No, thanks" "No eggs, no ham!"])
  ;    "No eggs, no ham!"
  ;  )
  ;(comment
  ;(javax.swing.JOptionPane/showInputDialog
  ;  nil
  ;  "Complete the sentence: Green eggs and..."
  ;  "Customized Dialog"
  ;  javax.swing.JOptionPane/PLAIN_MESSAGE
  ;  nil
  ;  possibilities
  ;  "ham"
  ;)
  ;)
 )

; (-main)
; (apply + (range 15))
