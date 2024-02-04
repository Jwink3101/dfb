# Compare Setting Guidance

## Default

Keep the defaults of `"auto"` for most. They are

    'compare'
    'dst_compare'
    'renames'
    'dst_renames'
    'get_modtime'
    'get_hashes'

Note that the auto is using "mtime" if and when possible for compare. If "mtime" is not possible, disable renames

## Comparison and Rename Attributes

The attributes for comparison and for renames are user settable. If both remotes support hashes, it almost always best to use them. And if the source is slow to list ModTime, you can also set `get_modtime = False`. If remotes support ModTime and it is fast, that is a decent choice for both compare and renames.

Generally speaking, comparisons and renames are actually source-to-source because the source values are saved. However, if run with `--refresh`, then comparisons and move-tracking are source-to-dest. In that case, you can set `dst_compare` and `dst_renames`. 

<table>
    <tr>
        <th>Source</th>
        <th>Destination</th>
        <th><code>compare</code></th>
        <th><code>dst_compare</code></th>
        <th><code>renames</code></th>
        <th><code>dst_renames</code></th>
        <th>Comment</th>
    </tr>
    <tr>
        <td>Local</td>
        <td>
            Any remote that supports ModTime including 
            local, B2, OneDrive, DropBox, [Google] Drive,
            [S]FTP, <strong>certain</strong> WebDAV. <strong>NOT</strong> S3.
        </td>
        <td>'mtime'</td>
        <td>None</td>
        <td>'mtime'</td>
        <td>None</td>
        <td>Use 'mtime' since it easy and fairly reliable</td>
    </tr>
        <td>Local</td>
        <td>
            S3, <em>Regular</em> WebDAV
        </td>
        <td>'mtime'</td>
        <td>'size'</td>
        <td>'mtime'</td>
        <td>False</td>
        <td>
            Use 'mtime' when using past source data but switch to size
            when using <code>dst_</code> since mtime is either super slow
            (S3) or unreliable (WebDAV). Since 'size' is a poor file tracker,
            disable renames in that case.
        </td>
    </tr>
    <tr>
        <td colspan="2">
            <strong>Identical</strong> cloud-to-cloud that support fast hashing (i.e. not SFTP). Or if the same hash is supported (e.g. S3 to Azure)
        </td>
        <td>'hash'</td>
        <td>None</td>
        <td>'hash'</td>
        <td>None</td>
        <td>Use 'hash' since it is fast and reliable</td>
    </tr>
    <tr>
        <td colspan="2">
            <strong>Different</strong> cloud-to-cloud that support fast hashing (i.e. not SFTP)
        </td>
        <td>'hash'</td>
        <td>'size'</td>
        <td>'hash'</td>
        <td>None</td>
        <td>
            Won't have hashes on both. Can change 'size' to 'mtime' if both 
            support it and one is not S3 (where 'mtime' is slow)
        </td>
    </tr>
    <tr>
        <td>Cloud that supports fast hashing</td>
        <td>Local or SFTP</td>
        <td>'hash'</td>
        <td>'size'</td>
        <td>'hash'</td>
        <td>None</td>
        <td>
            Can change 'size' to 'mtime' if  
            not S3 (where 'mtime' is slow)
        </td>
    </tr>
    <tr>
        <td>regular WebDAV, Mega, Seafile, any other non-mod-time remote</td>
        <td>Anything</td>
        <td>'size'</td>
        <td>None</td>
        <td>False</td>
        <td>False</td>
        <td>
            Only use size to detect changed files. Good enough most of the time but unreliable for move detection
        </td>
    </tr>
</table>

**NOTE**: Even if compares are *not* `'mtime'`, all restores will do `mtime` if possible!

## Alternative S3 source settings

If your source is S3 and you do not want to work off of "hash" for whatever reason (some non-rclone tools do not set it always) and don't want to fall back to 'size' or the slow 'mtime', one option is to add the flag `--use-server-modtime` and then `'mtime'` for the compare. This will detect changes just fine!
