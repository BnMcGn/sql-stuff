;;;; package.lisp

(defpackage #:sql-stuff
  (:use #:cl #:gadgets #:clsql #:clsql-helper #:alexandria)
  (:shadowing-import-from :clsql-helper :with-database)
  (:export
   #:get-table-pkey
   #:colm
   #:tabl
   #:table-from-attribute-obj
   #:col-from-attribute-obj

   ;query composition stuff
   #:merge-query
   #:add-count
   #:apply-car
   #:query-marker
   #:query-code-p
   #:limit-mixin
   #:order-by-mixin
   #:unexecuted
   #:m2m-mixin
   #:def-query
   #:sql-escape
   #:escape-error

   ;join stuff
   #:join-bind
   #:join-add
   #:get-pkeys-for-pkey
   #:update-pkeys-for-pkey
   #:get-pkeys-for-pkey/chain
   ;join variables
   #:table1
   #:pkey1
   #:fkey1
   #:join-table
   #:fkey2
   #:pkey2
   #:table2

   ;quick accessors
   #:get-record-by-pkey
   #:get-assoc-by-pkey
   #:update-record
   #:insert-record
   #:insert-or-update
   #:get-column
   #:get-columns
   #:col-from-pkey
   #:in-or-equal
   #:fulltext-search
   #:grab-one
   #:get-count
   #:get-tables
   #:assocify
   #:assocify-results
   #:exists
   #:get-assoc-by-col
   #:next-val
   #:sql-equal/null
   #:insert-plist
   #:colms
   #:get-table-columns))
