Gem::Specification.new do |s|
  s.name        = 'geminabox-s3-store'
  s.version     = '0.0.1'
  s.date        = '2014-12-07'
  s.summary     = "AWS S3 store for geminabox"
  s.description = "A simple hello world gem"
  s.authors     = ["Konstantin Burnaev"]
  s.email       = 'kbourn@gmail.com'
  s.files       = [
    "lib/geminabox-s3-store.rb",
    "lib/geminabox/store/s3.rb"
  ]
  s.homepage    =
    'https://github.com/bkon/geminabox-s3-store'
  s.license     = 'MIT'

  s.add_runtime_dependency 'geminabox'
end
