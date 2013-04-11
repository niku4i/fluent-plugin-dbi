module Fluent

class DbiOutput < BufferedOutput
  Plugin.register_output('dbi', self)

  config_param :dsn, :string
  config_param :keys, :string
  config_param :db_user, :string
  config_param :db_pass, :string
  config_param :query, :string
  config_param :time_format, :string, default: nil

  def initialize
    super

    require 'dbi'
  end

  def configure(conf)
    super

    @keys = @keys.split(",")
    @timef = TimeFormatter.new(@time_format, localtime = true)
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def write(chunk)
    begin
      dbh = DBI.connect(@dsn, @db_user, @db_pass)
      dbh['AutoCommit'] = false
      sth = dbh.prepare(@query)
      chunk.msgpack_each { |tag, time, record|
        if @time_format
          record['time'] = @timef.format(time)
        else
          record.key?('time') || record['time'] = time
        end
        record.key('tag') || record['tag'] = tag
        values = []
        @keys.each { |key|
          values.push(record[key])
        }
        rows = sth.execute(*values)
      }
    rescue
      dbh.rollback if dbh
      raise
    else
      sth.finish
      dbh.commit
    ensure
      dbh.disconnect if dbh
    end
  end
end

end
