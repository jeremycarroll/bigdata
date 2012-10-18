/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.ha;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.IReopenChannel;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockUtility;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.journal.WORMStrategy;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.ChecksumUtility;

/**
 * Given an HALog file can be used to replay the file and can provide a readable
 * dump of the content.
 * 
 * When replaying, the current position is compared to the EOF to determine
 * whether more data can be read.
 * 
 * The called should call hasMoreBuffers() and if so read the next associated
 * buffer and process with the returned IHAMessage.
 * 
 * If hasMoreBuffers() is false, then the committing rootBlock should be used to
 * commit the replayed transaction.
 * 
 * @author Martyn Cutcher
 */
public class HALogReader implements IHALogReader {

	private static final Logger log = Logger.getLogger(HALogReader.class);

	private final File m_file;
	private final RandomAccessFile m_raf;
	private final FileChannel m_channel;
	private final IRootBlockView m_openRootBlock;
	private final IRootBlockView m_closeRootBlock;
	private final StoreTypeEnum m_storeType;
	private final int magic;
	private final int version;

	public HALogReader(final File file) throws IOException {

		m_file = file;

		m_raf = new RandomAccessFile(file, "r");

		m_channel = m_raf.getChannel();

		try {
			/**
			 * Must determine whether the file has consistent open and committed
			 * rootBlocks, using the commitCounter to determine which rootBlock
			 * is which.
			 * 
			 * Note: Both root block should exist (they are both written on
			 * startup). If they are identical, then the log is empty (the
			 * closing root block has not been written and the data in the log
			 * is useless).
			 * 
			 * We figure out which root block is the opening root block based on
			 * standard logic.
			 */
			/*
			 * Read the MAGIC and VERSION.
			 */
			m_raf.seek(0L);
			try {
				/*
				 * Note: this next line will throw IOException if there is a
				 * file lock contention.
				 */
				magic = m_raf.readInt();
			} catch (IOException ex) {
				throw new RuntimeException(
						"Can not read magic. Is file locked by another process?",
						ex);
			}
			if (magic != HALogWriter.MAGIC)
				throw new RuntimeException("Bad journal magic: expected="
						+ HALogWriter.MAGIC + ", actual=" + magic);
			version = m_raf.readInt();
			if (version != HALogWriter.VERSION1)
				throw new RuntimeException("Bad journal version: expected="
						+ HALogWriter.VERSION1 + ", actual=" + version);

			final RootBlockUtility tmp = new RootBlockUtility(reopener, file,
					true/* validateChecksum */, false/* alternateRootBlock */,
					false/* ignoreBadRootBlock */);

			m_closeRootBlock = tmp.chooseRootBlock();

			m_openRootBlock = tmp.rootBlock0 == m_closeRootBlock ? tmp.rootBlock1
					: tmp.rootBlock0;

			final long cc0 = m_openRootBlock.getCommitCounter();

			final long cc1 = m_closeRootBlock.getCommitCounter();

			if ((cc0 + 1) != cc1 && (cc0 != cc1)) {
				/*
				 * Counters are inconsistent with either an empty log file or a
				 * single transaction scope.
				 */
				throw new IllegalStateException("Incompatible rootblocks: cc0="
						+ cc0 + ", cc1=" + cc1);
			}

			m_channel.position(HALogWriter.headerSize0);

			m_storeType = m_openRootBlock.getStoreType();

		} catch (Throwable t) {

			close();

			throw new RuntimeException(t);

		}

	}

	/**
	 * Hook for
	 * {@link FileChannelUtility#readAll(FileChannel, ByteBuffer, long)}
	 */
	private final IReopenChannel<FileChannel> reopener = new IReopenChannel<FileChannel>() {

		@Override
		public FileChannel reopenChannel() throws IOException {

			if (m_channel == null)
				throw new IOException("Closed");

			return m_channel;

		}
	};

	public void close() {

		if (m_channel.isOpen()) {

			try {
				m_raf.close();
			} catch (IOException e) {
				log
						.error("Problem closing file: file=" + m_file + " : "
								+ e, e);
			}

		}

	}

	/**
	 * Return <code>true</code> if the root blocks in the log file have the same
	 * commit counter. Such log files are logically empty regardless of their
	 * length.
	 */
	public boolean isEmpty() {

		return m_openRootBlock.getCommitCounter() == m_closeRootBlock
				.getCommitCounter();

	}

	private void assertOpen() throws IOException {

		if (!m_channel.isOpen())
			throw new IOException("Closed: " + m_file);

	}

	/**
	 * The {@link IRootBlockView} for the committed state BEFORE the write set
	 * contained in the HA log file.
	 */
	public IRootBlockView getOpeningRootBlock() {

		return m_openRootBlock;

	}

	/**
	 * The {@link IRootBlockView} for the committed state AFTER the write set
	 * contained in the HA log file has been applied.
	 */
	public IRootBlockView getClosingRootBlock() {

		return m_closeRootBlock;

	}

	/**
	 * Checks whether we have reached the end of the file.
	 */
	public boolean hasMoreBuffers() throws IOException {

		assertOpen();

		if (isEmpty()) {

			/*
			 * Ignore the file length if it is logically empty.
			 */

			return false;

		}

		return m_channel.position() < m_channel.size();

	}

	/**
	 * To stream from the Channel, we can use the associated RandomAccessFile
	 * since the FilePointer for one is the same as the other.
	 */
	private static class RAFInputStream extends InputStream {
		final RandomAccessFile m_raf;
		
		RAFInputStream(final RandomAccessFile raf) {
			m_raf = raf;
		}
		
		@Override
		public int read() throws IOException {
			return m_raf.read();
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			return m_raf.read(b, off, len);
		}

	}

	/**
	 * Attempts to read the next {@link IHAWriteMessage} and then the expected
	 * buffer, that is read into the client buffer. The {@link IHAWriteMessage}
	 * is returned to the caller.
	 * <p>
	 * Note: The caller's buffer will be filled in IFF the data is on the HALog.
	 * For some {@link IHABufferStrategy} implementations, that data is not
	 * present in the HALog. The caller's buffer will not be modified and the
	 * caller is responsible for getting the data from the
	 * {@link IHABufferStrategy} (e.g., for the {@link WORMStrategy}).
	 * <p>
	 * Note: IF the buffer is filled, then the limit will be the #of bytes ready
	 * to be transmitted and the position will be zero.
	 * 
	 * @param clientBuffer
	 *            A buffer from the {@link DirectBufferPool#INSTANCE}.
	 */
	public IHAWriteMessage processNextBuffer(final ByteBuffer clientBuffer)
			throws IOException {

		return processNextBuffer(m_raf, reopener, m_storeType, clientBuffer);

	}

	static public IHAWriteMessage processNextBuffer(final RandomAccessFile raf, final IReopenChannel<FileChannel> reopener,
			final StoreTypeEnum storeType, final ByteBuffer clientBuffer)
			throws IOException {

		final FileChannel channel = raf.getChannel();
		
		final ObjectInputStream objinstr = new ObjectInputStream(
				new RAFInputStream(raf));

		final IHAWriteMessage msg;
		try {

			msg = (IHAWriteMessage) objinstr.readObject();

		} catch (ClassNotFoundException e) {

			throw new IllegalStateException(e);

		}

		switch (storeType) {
		case RW: {

			if (msg.getSize() > clientBuffer.capacity()) {

				throw new IllegalStateException(
						"Client buffer is not large enough for logged buffer");

			}

			// Now setup client buffer to receive from the channel
			final int nbytes = msg.getSize();
			clientBuffer.position(0);
			clientBuffer.limit(nbytes);

			// Current position on channel.
			final long pos = channel.position();

			// allow null clientBuffer for IHAWriteMessage only
			if (clientBuffer != null) {
				// Robustly read of write cache block at that position into the
				// caller's buffer. (pos=limit=nbytes)
				FileChannelUtility.readAll(reopener, clientBuffer, pos);

				// limit=pos; pos=0;
				clientBuffer.flip(); // ready for reading

				final int chksum = new ChecksumUtility().checksum(clientBuffer
						.duplicate());

				if (chksum != msg.getChk())
					throw new ChecksumError("Expected=" + msg.getChk()
							+ ", actual=" + chksum);

				if (clientBuffer.remaining() != nbytes)
					throw new AssertionError();
			}
			// Advance the file channel beyond the block we just read.
			channel.position(pos + msg.getSize());

			break;
		}
		case WORM: {
			/*
			 * Note: The WriteCache block needs to be recovered from the
			 * WORMStrategy by the caller.  The clientBuffer, if supplied,
			 * is ignored and untouched.
			 * 
			 * It is permissible for the argument to be null.
			 */

			// final int nbytes = msg.getSize();
			// clientBuffer.position(0);
			// clientBuffer.limit(nbytes);
			//
			// final long address = m_addressManager.toAddr(nbytes, msg
			// .getFirstOffset());
			// final ByteBuffer src = m_bufferStrategy.read(address);
			//
			// clientBuffer.put(src);
			// }
			break;
		}
		default:
			throw new UnsupportedOperationException();
		}

		return msg;

	}

	/**
	 * Utility program will dump log files (or directories containing log files)
	 * provided as arguments.
	 * 
	 * @param args
	 *            Zero or more files or directories.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(final String[] args) throws IOException,
			InterruptedException {

		final IBufferAccess buf = DirectBufferPool.INSTANCE.acquire();

		try {

			for (String arg : args) {

				final File file = new File(arg);

				if (!file.exists()) {

					System.err.println("No such file: " + file);

					continue;

				}

				if (file.isDirectory()) {

					doDirectory(file, buf);

				} else {

					doFile(file, buf);

				}

			}

		} finally {

			buf.release();

		}

	}

	private static void doDirectory(final File dir, final IBufferAccess buf)
			throws IOException {

		final File[] files = dir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {

				if (new File(dir, name).isDirectory()) {

					// Allow recursion through directories.
					return true;

				}

				return name.endsWith(HALogWriter.HA_LOG_EXT);

			}
		});

		for (File file : files) {

			if (file.isDirectory()) {

				doDirectory(file, buf);

			} else {

				doFile(file, buf);

			}

		}

	}

	private static void doFile(final File file, final IBufferAccess buf)
			throws IOException {

		final HALogReader r = new HALogReader(file);

		try {

			final IRootBlockView openingRootBlock = r.getOpeningRootBlock();

			final IRootBlockView closingRootBlock = r.getClosingRootBlock();

			final boolean isWORM = openingRootBlock.getStoreType() == StoreTypeEnum.WORM;

			if (openingRootBlock.getCommitCounter() == closingRootBlock
					.getCommitCounter()) {

				System.err.println("EMPTY LOG: " + file);

			}

			System.out.println("----------begin----------");
			System.out.println("file=" + file);
			System.out.println("openingRootBlock=" + openingRootBlock);
			System.out.println("closingRootBlock=" + closingRootBlock);

			while (r.hasMoreBuffers()) {

				// don't pass buffer in if WORM, just validate the messages
				final IHAWriteMessage msg = r.processNextBuffer(isWORM ? null
						: buf.buffer());

				System.out.println(msg.toString());

			}
			System.out.println("-----------end-----------");

		} finally {

			r.close();

		}

	}

}
