module ActiveRecord
  module ConnectionAdapters
    module SQLServer
      module DatabaseStatements
        def sql_for_insert(sql, pk, binds)
          if pk.nil?
            table_name = query_requires_identity_insert?(sql)
            pk = primary_key(table_name)
          end

          sql = if pk && use_output_inserted? && !database_prefix_remote_server?
                  # CPK
                  #quoted_pk = SQLServer::Utils.extract_identifiers(pk).quoted
                  quoted_pk = Array(pk).map {|subkey| SQLServer::Utils.extract_identifiers(subkey).quoted}

                  table_name ||= get_table_name(sql)
                  exclude_output_inserted = exclude_output_inserted_table_name?(table_name, sql)
                  if exclude_output_inserted
                    column_sql_type_index = schema_cache.columns(table_name).each_with_object({}) do |column, hash|
                      hash[SQLServer::Utils.extract_identifiers(column.name).quoted] = exclude_output_inserted.is_a?(TrueClass) ? 'bigint' : exclude_output_inserted[column.name]
                    end
                    # CPK
                    # <<~SQL.squish
                    #   DECLARE @ssaIdInsertTable table (#{quoted_pk} #{id_sql_type});
                    #   #{sql.dup.insert sql.index(/ (DEFAULT )?VALUES/), " OUTPUT INSERTED.#{quoted_pk} INTO @ssaIdInsertTable"}
                    #   SELECT CAST(#{quoted_pk.join(',')} AS #{id_sql_type}) FROM @ssaIdInsertTable
                    # SQL
                    <<~SQL.squish
                      DECLARE @ssaIdInsertTable table (#{quoted_pk.map {|subkey| "#{subkey} #{column_sql_type_index[subkey]}"}.join(", ")});
                      #{sql.dup.insert sql.index(/ (DEFAULT )?VALUES/), " OUTPUT INSERTED.#{quoted_pk.join(', INSERTED.')} INTO @ssaIdInsertTable"}
                      SELECT #{quoted_pk.map {|subkey| "CAST(#{subkey} AS #{column_sql_type_index[subkey]}) #{subkey}"}.join(", ")} FROM @ssaIdInsertTable
                    SQL
                  else
                    # CPK
                    # sql.dup.insert sql.index(/ (DEFAULT )?VALUES/), " OUTPUT INSERTED.#{quoted_pk}"
                    sql.dup.insert sql.index(/ (DEFAULT )?VALUES/), " OUTPUT INSERTED.#{quoted_pk.join(', INSERTED.')}"
                  end
                else
                  "#{sql}; SELECT CAST(SCOPE_IDENTITY() AS bigint) AS Ident"
                end
          super
        end
      end
    end
  end
end
