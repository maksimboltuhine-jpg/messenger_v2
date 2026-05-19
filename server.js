const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const multer = require('multer');
const compression = require('compression');
const bcrypt = require('bcryptjs');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" }, maxHttpBufferSize: 1e8 });

app.use(compression());
app.use(express.json({ limit: '100mb' }));
app.use(express.static(__dirname));

const MONGO_URI = process.env.MONGO_URI || 'mongodb+srv://maksimboltuhine_db_user:Maksim12345@cluster0.peuxhxx.mongodb.net/chatDB?retryWrites=true&w=majority';

const User = mongoose.model('User', new mongoose.Schema({
    login: { type: String, unique: true, required: true },
    password: { type: String, required: true },
    uid: { type: String, unique: true },
    avatar: { type: String, default: '' },
    bio: { type: String, default: '' },
    displayName: { type: String, default: '' },
    status: { type: String, default: 'online' }
}));

const Group = mongoose.model('Group', new mongoose.Schema({
    name: String,
    groupId: { type: String, unique: true },
    owner: String,
    members: [String],
    isDirect: { type: Boolean, default: false },
    avatar: { type: String, default: '' },
    description: { type: String, default: '' }
}));

const Msg = mongoose.model('Msg', new mongoose.Schema({
    user: String, uid: String, text: String, room: String,
    fileUrl: String, fileId: String, fileType: String, fileName: String,
    isVideo: { type: Boolean, default: false },
    isVoice: { type: Boolean, default: false },
    replyTo: { type: String, default: '' },
    edited: { type: Boolean, default: false },
    createdAt: { type: Date, default: Date.now, expires: 86400 }
}));

const JoinRequest = mongoose.model('JoinRequest', new mongoose.Schema({
    groupId: String,
    fromUid: String,
    fromName: String,
    status: { type: String, default: 'pending' },
    createdAt: { type: Date, default: Date.now, expires: 86400 * 7 }
}));

let gfsBucket;
mongoose.connect(MONGO_URI).then(() => {
    console.log('DATABASE ONLINE');
    gfsBucket = new mongoose.mongo.GridFSBucket(mongoose.connection.db, { bucketName: 'uploads' });
});

app.post('/auth', async (req, res) => {
    const { login, password, isReg } = req.body;
    try {
        let user = await User.findOne({ login });
        if (isReg) {
            if (user) return res.status(400).json({ error: "Логин занят" });
            const hash = await bcrypt.hash(password, 7);
            const uid = '#' + Math.floor(1000 + Math.random() * 9000);
            user = new User({ login, password: hash, uid, displayName: login });
            await user.save();
        } else {
            if (!user || !(await bcrypt.compare(password, user.password))) return res.status(400).json({ error: "Ошибка входа" });
        }
        res.json({ login: user.login, uid: user.uid, avatar: user.avatar, bio: user.bio, displayName: user.displayName || user.login, status: user.status });
    } catch (e) { res.status(500).json({ error: "Ошибка сервера" }); }
});

app.get('/user/:uid', async (req, res) => {
    try {
        const user = await User.findOne({ uid: req.params.uid }, 'login uid avatar bio displayName status');
        if (user) res.json(user); else res.status(404).json({ error: "Не найден" });
    } catch (e) { res.status(500).json({ error: "Ошибка" }); }
});

app.post('/update_profile', async (req, res) => {
    const { uid, avatar, bio, displayName, status } = req.body;
    const upd = {};
    if (avatar !== undefined) upd.avatar = avatar;
    if (bio !== undefined) upd.bio = bio;
    if (displayName !== undefined) upd.displayName = displayName;
    if (status !== undefined) upd.status = status;
    const user = await User.findOneAndUpdate({ uid }, upd, { new: true });
    if (!user) return res.status(404).json({ error: "Не найден" });
    res.json({ login: user.login, uid: user.uid, avatar: user.avatar, bio: user.bio, displayName: user.displayName, status: user.status });
});

app.get('/group/:groupId/members', async (req, res) => {
    try {
        const group = await Group.findOne({ groupId: req.params.groupId });
        if (!group) return res.status(404).json({ error: "Группа не найдена" });
        const members = await User.find({ uid: { $in: group.members } }, 'login uid avatar bio displayName status');
        res.json({ group, members });
    } catch (e) { res.status(500).json({ error: "Ошибка" }); }
});

app.get('/group_info/:groupId', async (req, res) => {
    try {
        const group = await Group.findOne({ groupId: req.params.groupId });
        if (!group || group.isDirect) return res.status(404).json({ error: "Группа не найдена" });
        res.json({ groupId: group.groupId, name: group.name, description: group.description, avatar: group.avatar, memberCount: group.members.length });
    } catch (e) { res.status(500).json({ error: "Ошибка" }); }
});

app.post('/update_group', async (req, res) => {
    const { groupId, ownerUid, name, avatar, description } = req.body;
    const group = await Group.findOne({ groupId, owner: ownerUid });
    if (!group) return res.status(403).json({ error: "Нет прав" });
    if (name) group.name = name;
    if (avatar !== undefined) group.avatar = avatar;
    if (description !== undefined) group.description = description;
    await group.save();
    res.json(group);
});

const upload = multer({ dest: 'uploads/' });
app.post('/upload', upload.single('file'), (req, res) => {
    if (!gfsBucket || !req.file) return res.sendStatus(500);
    let name = req.file.originalname;
    try { name = Buffer.from(name, 'latin1').toString('utf8'); } catch (e) {}
    const writeStream = gfsBucket.openUploadStream(name, { contentType: req.file.mimetype });
    fs.createReadStream(req.file.path).pipe(writeStream).on('finish', () => {
        fs.promises.unlink(req.file.path);
        res.json({ fileUrl: `/file/${writeStream.id}`, fileId: writeStream.id.toString(), fileType: req.file.mimetype, fileName: name });
    });
});

app.get('/file/:id', (req, res) => {
    try { gfsBucket.openDownloadStream(new mongoose.Types.ObjectId(req.params.id)).pipe(res); } catch (e) { res.sendStatus(404); }
});

const online = new Map();
io.on('connection', (socket) => {

    socket.on('set_online', async (u) => {
        online.set(socket.id, u);
        io.emit('update_online', Array.from(online.values()));
        socket.emit('my_groups', await Group.find({ members: u.uid }));
        const ownedGroups = await Group.find({ owner: u.uid });
        const groupIds = ownedGroups.map(g => g.groupId);
        if (groupIds.length > 0) {
            const pending = await JoinRequest.find({ groupId: { $in: groupIds }, status: 'pending' });
            if (pending.length > 0) socket.emit('pending_requests', pending);
        }
    });

    socket.on('create_group', async (d) => {
        const gid = 'room_' + Math.random().toString(36).substr(2, 9);
        await new Group({ name: d.name, groupId: gid, owner: d.uid, members: [d.uid], description: d.description || '' }).save();
        socket.emit('my_groups', await Group.find({ members: d.uid }));
    });

    socket.on('add_to_group', async (d) => {
        const g = await Group.findOne({ groupId: d.groupId, owner: d.ownerUid });
        if (g && !g.members.includes(d.targetUid)) {
            g.members.push(d.targetUid); await g.save();
            for (let [id, u] of online) if (u.uid === d.targetUid) io.to(id).emit('my_groups', await Group.find({ members: u.uid }));
            io.to(d.groupId).emit('group_updated', g);
        }
    });

    socket.on('request_join', async (d) => {
        const g = await Group.findOne({ groupId: d.groupId });
        if (!g || g.isDirect) return socket.emit('join_error', 'Группа не найдена');
        if (g.members.includes(d.fromUid)) return socket.emit('join_error', 'Вы уже в группе');
        const existing = await JoinRequest.findOne({ groupId: d.groupId, fromUid: d.fromUid, status: 'pending' });
        if (existing) return socket.emit('join_error', 'Заявка уже отправлена');
        const req = await new JoinRequest({ groupId: d.groupId, fromUid: d.fromUid, fromName: d.fromName }).save();
        for (let [id, u] of online) {
            if (u.uid === g.owner) io.to(id).emit('join_request', { _id: req._id, groupId: g.groupId, groupName: g.name, fromUid: d.fromUid, fromName: d.fromName });
        }
        socket.emit('join_requested', { groupName: g.name });
    });

    socket.on('handle_join', async (d) => {
        const req = await JoinRequest.findById(d.requestId);
        if (!req) return;
        const g = await Group.findOne({ groupId: req.groupId, owner: d.ownerUid });
        if (!g) return;
        if (d.accept) {
            req.status = 'accepted';
            if (!g.members.includes(req.fromUid)) { g.members.push(req.fromUid); await g.save(); }
            for (let [id, u] of online) {
                if (u.uid === req.fromUid) {
                    io.to(id).emit('join_accepted', { groupId: g.groupId, groupName: g.name });
                    io.to(id).emit('my_groups', await Group.find({ members: u.uid }));
                }
            }
        } else {
            req.status = 'rejected';
            for (let [id, u] of online) if (u.uid === req.fromUid) io.to(id).emit('join_rejected', { groupName: g.name });
        }
        await req.save();
    });

    socket.on('kick_member', async (d) => {
        const g = await Group.findOne({ groupId: d.groupId, owner: d.ownerUid });
        if (!g) return;
        g.members = g.members.filter(m => m !== d.targetUid);
        await g.save();
        for (let [id, u] of online) {
            if (u.uid === d.targetUid) {
                io.to(id).emit('kicked_from_group', { groupId: d.groupId });
                io.to(id).emit('my_groups', await Group.find({ members: u.uid }));
            }
        }
        io.to(d.groupId).emit('group_updated', g);
    });

    socket.on('delete_group', async (d) => {
        const g = await Group.findOne({ groupId: d.groupId, owner: d.ownerUid });
        if (!g) return;
        const members = [...g.members];
        await Group.deleteOne({ groupId: d.groupId });
        await Msg.deleteMany({ room: d.groupId });
        for (const uid of members) {
            for (let [id, u] of online) {
                if (u.uid === uid) {
                    io.to(id).emit('group_deleted', { groupId: d.groupId });
                    io.to(id).emit('my_groups', await Group.find({ members: u.uid }));
                }
            }
        }
    });

    socket.on('start_dm', async ({ myUid, targetUid }) => {
        let g = await Group.findOne({ isDirect: true, members: { $all: [myUid, targetUid] } });
        if (!g) {
            const t = await User.findOne({ uid: targetUid });
            if (!t) return;
            const dmName = t.displayName || t.login;
            g = new Group({ name: dmName, groupId: 'dm_' + Math.random().toString(36).substr(2, 9), owner: 'system', members: [myUid, targetUid], isDirect: true });
            await g.save();
            for (let [id, u] of online) if (u.uid === targetUid) io.to(id).emit('my_groups', await Group.find({ members: u.uid }));
        }
        socket.emit('force_join_dm', g);
        socket.emit('my_groups', await Group.find({ members: myUid }));
    });

    socket.on('join', async (r) => {
        socket.rooms.forEach(rm => socket.leave(rm));
        socket.join(r);
        socket.emit('history', await Msg.find({ room: r }).sort({ createdAt: 1 }).limit(50));
    });

    socket.on('message', async (d) => {
        const m = new Msg(d); await m.save();
        io.to(d.room).emit('renderMsg', { ...d, _id: m._id, createdAt: m.createdAt });
    });

    socket.on('delete_msg', async (d) => {
        const msg = await Msg.findById(d.msgId);
        if (!msg || m


sg.uid !== d.uid) return;
        await Msg.deleteOne({ _id: d.msgId });
        io.to(msg.room).emit('msg_deleted', { msgId: d.msgId });
    });

    socket.on('edit_msg', async (d) => {
        const msg = await Msg.findById(d.msgId);
        if (!msg || msg.uid !== d.uid) return;
        msg.text = d.newText;
        msg.edited = true;
        await msg.save();
        io.to(msg.room).emit('msg_edited', { msgId: d.msgId, newText: d.newText });
    });

    socket.on('disconnect', () => {
        online.delete(socket.id);
        io.emit('update_online', Array.from(online.values()));
    });
});

server.listen(process.env.PORT || 10000, '0.0.0.0');