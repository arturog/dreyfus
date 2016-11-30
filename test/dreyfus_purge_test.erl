% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(dreyfus_purge_test).

-include_lib("couch/include/couch_db.hrl").
-include_lib("dreyfus/include/dreyfus.hrl").

-export([test_purge_single/0, test_purge_multiple/0, test_purge_multiple2/0]).


test_purge_single() ->
    couch_log:notice("[~p] =============TEST CASE test_purge_single=============", [?MODULE]),

    couch_log:notice("[~p] create the db and docs", [?MODULE]),
    DbName = db_name(),
    create_db_docs(DbName),

    couch_log:notice("[~p] first search request", [?MODULE]),
    {ok, _, 1, _, _, _} = dreyfus_search(DbName, <<"apple">>),

    couch_log:notice("[~p] purge one doc", [?MODULE]),
    DBFullName = get_full_dbname(DbName),
    couch_log:notice("[~p] DBFullName:~p", [?MODULE, DBFullName]),
    {ok, _, _} = purge_one_doc(DBFullName, <<"apple">>),

    couch_log:notice("[~p] second search request", [?MODULE]),
    {ok, _, 0, _, _, _} = dreyfus_search(DbName, <<"apple">>),

    couch_log:notice("[~p] delete the db", [?MODULE]),
    delete_db(DbName),

    couch_log:notice("[~p] test_purge_single finished.", [?MODULE]),
    ok.


test_purge_multiple() ->
    couch_log:notice("[~p] =============TEST CASE test_purge_multiple=============", [?MODULE]),

    couch_log:notice("[~p] create the db and docs", [?MODULE]),
    DbName = db_name(),
    create_db_docs(DbName),

    Query = <<"color:red">>,

    couch_log:notice("[~p] first search request", [?MODULE]),
    {ok, _, 5, _, _, _} = dreyfus_search(DbName, Query),

    DBFullName = get_full_dbname(DbName),
    couch_log:notice("[~p] DBFullName:~p", [?MODULE, DBFullName]),

    couch_log:notice("[~p] purge 1/5 doc, apple", [?MODULE]),
    {ok, _, _} = purge_one_doc(DBFullName, <<"apple">>),
    couch_log:notice("[~p] purge 2/5 doc, tomato", [?MODULE]),
    {ok, _, _} = purge_one_doc(DBFullName, <<"tomato">>),
    couch_log:notice("[~p] purge 3/5 doc, cherry", [?MODULE]),
    {ok, _, _} = purge_one_doc(DBFullName, <<"cherry">>),
    couch_log:notice("[~p] purge 4/5 doc, haw", [?MODULE]),
    {ok, _, _} = purge_one_doc(DBFullName, <<"haw">>),
    couch_log:notice("[~p] purge 5/5 doc, strawberry", [?MODULE]),
    {ok, _, _} = purge_one_doc(DBFullName, <<"strawberry">>),

    couch_log:notice("[~p] second search request", [?MODULE]),
    {ok, _, 0, _, _, _} = dreyfus_search(DbName, Query),


    couch_log:notice("[~p] delete the db", [?MODULE]),
    delete_db(DbName),

    couch_log:notice("[~p] test_purge_multiple finished.", [?MODULE]),
    ok.


test_purge_multiple2() ->
    couch_log:notice("[~p] =============TEST CASE test_purge_multiple2=============", [?MODULE]),

    couch_log:notice("[~p] create the db and docs", [?MODULE]),
    DbName = db_name(),
    create_db_docs(DbName),

    Query = <<"color:red">>,

    couch_log:notice("[~p] first search request", [?MODULE]),
    {ok, _, 5, _, _, _} = dreyfus_search(DbName, Query),

    DBFullName = get_full_dbname(DbName),
    couch_log:notice("[~p] DBFullName:~p", [?MODULE, DBFullName]),

    couch_log:notice("[~p] purge 1/5 doc, apple", [?MODULE]),
    {ok, _, _} = purge_one_doc(DBFullName, <<"apple">>),
    couch_log:notice("[~p] purge 2/5 doc, tomato", [?MODULE]),
    {ok, _, _} = purge_one_doc(DBFullName, <<"tomato">>),

    couch_log:notice("[~p] second search request", [?MODULE]),
    {ok, _, 3, _, _, _} = dreyfus_search(DbName, Query),

    couch_log:notice("[~p] purge 3/5 doc, cherry", [?MODULE]),
    {ok, _, _} = purge_one_doc(DBFullName, <<"cherry">>),
    couch_log:notice("[~p] purge 4/5 doc, haw", [?MODULE]),
    {ok, _, _} = purge_one_doc(DBFullName, <<"haw">>),

    couch_log:notice("[~p] third search request", [?MODULE]),
    {ok, _, 1, _, _, _} = dreyfus_search(DbName, Query),


    couch_log:notice("[~p] delete the db", [?MODULE]),
    delete_db(DbName),

    couch_log:notice("[~p] test_purge_multiple2 finished.", [?MODULE]),
    ok.


%private API
db_name() ->
    Nums = tuple_to_list(erlang:now()),
    Prefix = "test-db",
    Suffix = lists:concat([integer_to_list(Num) || Num <- Nums]),
    list_to_binary(Prefix ++ "-" ++ Suffix).

purge_one_doc(DBFullName, DocId) ->
    {ok, Db} = couch_db:open_int(DBFullName, []),
    FDI = couch_db:get_full_doc_info(Db, DocId),
    #doc_info{ revs = [#rev_info{} = PrevRev | _] } = couch_doc:to_doc_info(FDI),
    Rev = PrevRev#rev_info.rev,
    couch_db:purge_docs(Db, {DocId, [Rev]}).

dreyfus_search(DbName, KeyWord) ->
    QueryArgs = #index_query_args{q = KeyWord},
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/search">>, []),
    dreyfus_fabric_search:go(DbName, DDoc, <<"index">>, QueryArgs).

create_db_docs(DbName) ->
    ok = fabric:create_db(DbName, [?ADMIN_CTX]),
    {ok, _} = fabric:update_docs(DbName, make_docs(5), [?ADMIN_CTX]),
    {ok, _} = fabric:update_doc(DbName, make_design_doc(dreyfus), [?ADMIN_CTX]).

delete_db(DbName) ->
    ok = fabric:delete_db(DbName, [?ADMIN_CTX]).


make_docs(Count) ->
    [make_doc(I) || I <- lists:seq(1, Count)].

make_doc(Id) ->
    couch_doc:from_json_obj({[
        {<<"_id">>, get_value(Id)},
        {<<"color">>, <<"red">>},
        {<<"version">>, Id}
    ]}).

get_value(Key) ->
    case Key of
        1 -> <<"apple">>;
        2 -> <<"tomato">>;
        3 -> <<"cherry">>;
        4 -> <<"strawberry">>;
        5 -> <<"haw">>
    end.

get_full_dbname(DbName) ->
    Suffix = lists:sublist(binary_to_list(DbName), 9, 10),
    %Suffix1 = lists:sublist(binary_to_list(DbName), 15, 4),
    %Suffix2 = lists:sublist(binary_to_list(DbName), 19, 5),
    %Suffix = Suffix1 ++ "0" ++ Suffix2,
    DBFullName = "shards/00000000-ffffffff/" ++ binary_to_list(DbName) ++ "." ++ Suffix,
    DBFullName.


make_design_doc(dreyfus) ->
    couch_doc:from_json_obj({[
        {<<"_id">>, <<"_design/search">>},
        {<<"language">>, <<"javascript">>},
        {<<"indexes">>, {[
            {<<"index">>, {[
                {<<"analyzer">>, <<"standard">>},
                {<<"index">>, <<
                    "function (doc) { \n"
                    "  index(\"default\", doc._id);\n"
                    "  if(doc.color) {\n"
                    "    index(\"color\", doc.color);\n"
                    "  }\n"
                    "  if(doc.version) {\n"
                    "    index(\"version\", doc.version);\n"
                    "  }\n"
                    "}"
                >>}
            ]}}
        ]}}
    ]}).

