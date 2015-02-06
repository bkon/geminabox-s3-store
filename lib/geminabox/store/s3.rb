module Geminabox
  module Store
    class S3
      attr_reader :logger

      SPLICEABLE_GZIPPED_FILES = %w[
        specs.4.8.gz
        latest_specs.4.8.gz
        prerelease_specs.4.8.gz
      ]
      SPLICEABLE_TEXT_FILES = %w[
        yaml
        Marshal.4.8
        specs.4.8
        latest_specs.4.8
        prerelease_specs.4.8
      ]

      def initialize(bucket: nil, lock_manager: lock_manager, file_store: Geminabox::GemStore, logger: Logger.new(STDOUT))
        @bucket = bucket
        @file_store = file_store
        @lock_manager = lock_manager
        @logger = logger
      end

      def create(gem, overwrite = false)
        @file_store.create gem, overwrite

        object_name = gem_object_name("/gems/" + gem.name)

        logger.info "Gem: local -> S3 #{object_name}"

        @bucket
          .objects[object_name]
          .write gem.gem_data
        update_metadata
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

            update_metadata
          end
        end

        @file_store.update_local_file path_info
      end

      def update_local_metadata_file(path_info)
        file_name = File.basename path_info
        pull_file file_name do |local, remote|
          if file_name =~ /\.gz$/
            merge_gzipped local, remote
          else
            merge_text local, remote
          end
        end
      end

      def reindex &block
        FileUtils.mkpath @file_store.local_path "gems"
        @bucket.objects.with_prefix("gems/").each do |object|
          path_info = "/" + object.key
          local_file_path = @file_store.local_path path_info

          file_does_not_exist = !File.exists?(local_file_path)

          # File.size raises an exception if file does not exist
          file_size = file_does_not_exists ? 0 : File.size(local_file_path)
          file_has_different_size = object.content_length != file_size

          if file_does_not_exist || file_has_different_size
            logger.info "Gem: S3 -> local #{local_file_path}"
            File.write local_file_path, object.read
          end
        end

        @file_store.reindex(&block)
      end

      private

      def update_metadata
        @lock_manager.lock ".metadata" do
          push_files SPLICEABLE_GZIPPED_FILES do |local_contents, remote_contents|
            merge_gzipped local_contents, remote_contents
          end

          push_files SPLICEABLE_TEXT_FILES do |local_contents, remote_contents|
            merge_text local_contents, remote_contents
          end
        end
      end

      def push_files file_list, &block
        file_list.each do |file_name|
          push_file file_name, &block
        end
      end

      def merge_file_with_remote file_name
        local_index_file = @file_store.local_path file_name
        if File.exists? local_index_file
          old_contents = File.read(local_index_file, open_args: ["rb"])
        else
          old_contents = ''
        end

        object = s3_object(file_name)
        unless object.exists?
          old_contents
        else
          yield old_contents, object.read
        end
      end

      def s3_object file_name
        object_name = metadata_object_name('/' + file_name)
        @bucket.objects[object_name]
      end

      def push_file file_name, &block
        logger.info "Push: local -> S3 #{file_name}"

        new_contents = merge_file_with_remote file_name, &block
        s3_object(file_name).write new_contents
      end

      def pull_file file_name, &block
        logger.info "Pull: S3 -> local #{file_name}"

        new_contents = merge_file_with_remote file_name, &block
        file_path = @file_store.local_path file_name
        File.write file_path, new_contents
      end

      def gem_object_name(path_info)
        # Remove loading slash from the path
        path_info[1..-1]
      end

      def metadata_object_name(path_info)
        path_info.gsub %r{/}, 'metadata/'
      end

      def merge_gzipped(a, b)
        package(unpackage(a) | unpackage(b))
      end

      def merge_text(a, b)
        a.to_s + b.to_s
      end

      def unpackage(content)
        Marshal.load(Gem.gunzip(content))
      end

      def package(content)
        Gem.gzip(Marshal.dump(content))
      end
    end
  end
end
