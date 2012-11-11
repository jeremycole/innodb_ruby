class Innodb::Space
  PAGE_SIZE = 16384

  def initialize(file)
    @file = File.open(file)
    @size = @file.stat.size
    @pages = (@size/PAGE_SIZE)
  end

  def page(page_number)
    offset = page_number.to_i * PAGE_SIZE
    return nil unless offset < @size
    return nil unless (offset+PAGE_SIZE) <= @size
    @file.seek(offset)
    page_data = @file.read(PAGE_SIZE)
    Innodb::Page.new(page_data)
  end

  def each_page
    (0...@pages).each do |page_number|
      current_page = page(page_number)
      yield page_number, current_page if current_page
    end
  end
end
