module Geminabox
  module Store
    class S3
      def initialize(bucket: nil, file_store: Geminabox::GemStore)
        @bucket = bucket
        @file_store = file_store
      end

      def create(gem, overwrite = false)
        @file_store.create gem, overwrite

        @bucket
          .objects[gem_object_name(path_info)]
          .write gem.gem_data
      end

      # Note: deleting doesn't make much sense in this case anyway, as
      # other instances  will continue  serving cached copies  of this
      # gem (there's no way to notify them that gem has been deleted)
      #
      # Do consider using Geminabox.allow_delete = false
      #
      def delete(path_info)
        @file_store.delete path_info

        @bucket.objects[gem_object_name(path_info)].delete
      end

      def update_local_file(path_info)
        gem_file = @file_store.local_path path_info

        unless File.exists? gem_file
          gem_object = @bucket.objects[gem_object_name(path_info)]
          if gem_object.exists?
            # Note: this will load the entire contents of the gem into
            # memory  We  might  switch   to  using  streaming  IO  or
            # temporary files  if this  proves to  be critical  in our
            # environment
            io = StringIO.new gem_object.read
            incoming_gem = Geminabox::IncomingGem.new io
            @file_store.create incoming_gem
          end
        end

        @file_store.update_local_file path_info
      end

      private

      def gem_object_name(path_info)
        path_info
      end
    end
  end
end
