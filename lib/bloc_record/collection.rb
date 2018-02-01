module BlocRecord
  class Collection < Array

    def update_all(updates)
      ids = self.map(&:id)

      self.any? ? self.first.class.update(ids, updates) : false
    end

    def take(num = 1)
      new_collection = BlocRecord::Collection.new
      num.times do
        new_collection << self.shift
      end
      new_collection
    end

    def where(arg)
      # return an array that includes any items where arg key/value pair
      # matches
      self.map do |item|
        item if item[arg.key] == arg.value
      end
    end

    def not(arg)
      self.map {|item| item if item[arg.key] != arg.value }
    end
  end
end
