# encoding: utf-8

module Elasticsearchable
  extend ActiveSupport::Concern

  module ClassMethods
    # 创建index 新建index时使用
    # opts[:index_name] string, index_name
    # opts[:alias_name] string, index alias_name
    def es_create_index(opts = {})
      index_name = opts[:index_name] || self.index_name
      alias_name = opts[:alias_name]
      indices = self.__elasticsearch__.client.indices

      if indices.exists?(index: index_name)
        msg = "elasticsearch index already exists: #{index_name}"
        Rails.logger.error msg
        puts msg
        return
      else
        indices.create index: index_name, body: { settings: { :"index.mapper.dynamic" => false } }
        indices.put_alias(index: index_name, name: alias_name) if alias_name.present?
      end
    end

    # 刷新mapping 使用新建type时使用
    def es_refresh_mapping
      indices = self.__elasticsearch__.client.indices
      unless indices.exists?(index: self.index_name)
        msg = "elasticsearch index not exists: #{self.index_name}"
        Rails.logger.error msg
        puts msg
        return
      end

      mappings = self.mappings.to_hash
      # 默认strict
      mappings[self.document_type.to_sym][:dynamic] = 'strict' if mappings[self.document_type.to_sym][:dynamic].nil?

      indices.put_mapping index: self.index_name, type: self.document_type, body: mappings
    end

    # 获取elasticsearch中mapping信息
    def es_get_mapping
      indices = self.__elasticsearch__.client.indices
      unless indices.exists?(index: index_name)
        puts "#{index_name} not exists"
        return
      end
      indices.get_mapping(index: index_name, type: document_type)
    end

  end

  included do

    after_commit on: :create do
      es_index
    end
    after_commit on: :update do
      es_update
    end
    after_commit on: :destroy do
      es_delete
    end
  end

  def es_id
    self.id
  end

  def es_routing
    self.id
  end


  # 查询elasticsearch中是否存在符合es_id的记录
  def es_exists?
    __elasticsearch__.client.exists?(id: self.es_id, index: self.class.index_name, routing: es_routing)
  end

  def es_index
    __elasticsearch__.index_document(id: self.es_id, routing: es_routing)
  end
  def es_update
    # __elasticsearch__.update_document(id: self.es_id, routing: es_routing)
    # update_document会比对@__changed_attributes和as_indexed_json的key, 当as_indexed_json中的key有非active record的字段时会漏刷
    __elasticsearch__.update_document_attributes(self.as_indexed_json, id: self.es_id, routing: es_routing)
  end
  def es_delete
    Rails.logger.info "#{self.class.name} es_delete, id: #{self.id}"
    __elasticsearch__.delete_document(id: self.es_id, routing: es_routing)
  end


end
