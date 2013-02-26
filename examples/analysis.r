page_info = "edges_1_0025_edges" ; page_info_width=4000
page_info = "edges_test_d0p" ; page_info_width=1000
page_info = "edges_test_d1p" ; page_info_width=1000
page_info = "edges_test_d1p_a1p" ; page_info_width=1000
page_info = "t" ; page_info_width=1000

page_info_base = paste("~/git/innodb_ruby/", page_info, sep="")
p <- read.table(paste(page_info_base, ".page_info", sep=""), header=TRUE)

colors = c()
colors[1] = "black"
colors[p$index[1]+1] = "red"
colors[p$index[2]+1] = "green"
levels = c(46, 20, 20, 20, 20, 20, 20)

png(filename=paste(page_info_base, "_used.png", sep=""), width=page_info_width, height=800)
plot(p$data ~ p$page, pch=levels[p$level+1], xlab="page number", ylab="data per page", main=page_info, col=colors[p$index+1], xaxs="i", yaxp=c(0,16384,8), ylim=c(0,18000))
legend("topleft", c("PRIMARY", "unique_source_id_destination_id", "(unallocated pages)"), col=c("red", "green", "black"), pch=15)
dev.off()

png(filename=paste(page_info_base, "_free.png", sep=""), width=page_info_width, height=800)
plot(p$free ~ p$page, pch=levels[p$level+1], xlab="page number", ylab="free space per page", main=page_info, col=colors[p$index+1], xaxs="i", yaxp=c(0,16384,8), ylim=c(0,18000))
legend("topleft", c("PRIMARY", "unique_source_id_destination_id", "(unallocated pages)"), col=c("red", "green", "black"), pch=15)
dev.off()

png(filename=paste(page_info_base, "_cumulative_free.png", sep=""), width=page_info_width, height=800)
plot((cumsum(p$free)/1024^2) ~ p$page, xlab="page number", ylab="cumulative free space (MB)", main=page_info, col=colors[p$index+1], xaxs="i", pch=".", cex=4)
legend("topleft", c("PRIMARY", "unique_source_id_destination_id", "(unallocated pages)"), col=c("red", "green", "black"), pch=15)
dev.off()

h320 <- read.table("~/git/innodb_ruby/edges_1_0025_edges_320.histogram", header=FALSE)
h321 <- read.table("~/git/innodb_ruby/edges_1_0025_edges_321.histogram", header=FALSE)

png(filename="~/git/innodb_ruby/edges_1_0025_edges_hist.png", width=1600, height=800)
plot(h320$V2, h320$V1, type="s", ylab="number of pages (log scale)", xlab="data per page", log="y", ylim=c(1, 100000), xlim=c(0,16384), col="red")
lines(h321$V2, h321$V1, type="s", col="green")
legend("topleft", c("PRIMARY", "unique_source_id_destination_id"), col=c("red", "green"), pch=15)
dev.off()