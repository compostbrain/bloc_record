module BlocRecord
  class Collection < Array

    def update_all(updates)
      ids = self.map(&:id)

      self.any? ? self.first.class.update(ids, updates) : false
    end

    def take(num = 1)
      self.sample(num)
    end

    def where(arg)
      # return an array that includes any items where arg key/value pair
      # matches
      self.map do |item|

        item if item[arg.keys.first] == arg.values.first

      end
    end

    def not(arg)

      self.map {|item| item if item[arg.keys.first] != arg.values.first }

    end

    def destroy_all
      self.each { |item| item.destroy }
    end
  end
end
